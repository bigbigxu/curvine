use bytes::BytesMut;
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path, Reader, RpcCode};
use curvine_model::{ContentSummary, ContentSummarySource};
use curvine_unified_fs::UnifiedFileSystem;
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub(super) fn current_process_acl(client: &UnifiedFileSystem) -> (String, String, u32) {
    let uid = curvine_sys::get_uid();
    let gid = curvine_sys::get_gid();
    (
        curvine_sys::get_username_by_uid(uid).unwrap_or_else(|| uid.to_string()),
        curvine_sys::get_groupname_by_gid(gid).unwrap_or_else(|| gid.to_string()),
        client.conf().client.get_mode(),
    )
}

fn merge_live_summary(into: &mut ContentSummary, other: &ContentSummary) {
    into.logical_bytes = into.logical_bytes.saturating_add(other.logical_bytes);
    into.replicated_bytes = into.replicated_bytes.saturating_add(other.replicated_bytes);
    into.file_count = into.file_count.saturating_add(other.file_count);
    into.directory_count = into.directory_count.saturating_add(other.directory_count);
}

/// Max concurrent subtree walks of the live scan. Permits are handed to child
/// tasks (never held across awaiting them), so any depth is safe.
const LIVE_WALK_CONCURRENCY: usize = 16;

fn live_file_summary(len: i64, replicas: i32) -> ContentSummary {
    ContentSummary::live_file(len.max(0) as u64, replicas)
}

/// Live recursive traversal over the namespace the client is configured for.
///
/// The walk follows the client's own mode: with the unified layer enabled (no
/// `--cache-only`) cache-mode mounts are measured through their UFS storage, so
/// the summary covers CV + UFS data; with `--cache-only` the client
/// short-circuits to the CV namespace and the summary is CV-only.
///
/// Children listed through UFS carry UFS paths (e.g. `oss://bucket/...`); each
/// child is mapped back to its CV path through the parent's mount before
/// recursing, so the unified client keeps applying its delegation rules.
pub async fn calculate_content_summary_live(
    client: &UnifiedFileSystem,
    path: &Path,
) -> FsResult<ContentSummary> {
    let client = Arc::new(client.clone());
    let path = path.clone();
    let semaphore = Arc::new(Semaphore::new(LIVE_WALK_CONCURRENCY));
    walk_live(client, path, semaphore).await
}

/// Maps a child path reported by `list_status(parent)` back to its CV path.
/// UFS-backed listings report UFS paths; without this mapping the recursion
/// would feed `oss://...` paths back into the unified client and fail with
/// "path is not curvine path".
async fn child_cv_path(
    client: &UnifiedFileSystem,
    parent: &Path,
    raw: Path,
) -> FsResult<Path> {
    if raw.is_cv() {
        return Ok(raw);
    }
    match client.get_mount(parent, RpcCode::ListStatus).await? {
        Some((_, mount)) => Ok(mount.get_cv_path(&raw)?),
        None => Ok(raw),
    }
}

/// Boxed recursion: the explicit `+ Send` bound lets the compiler verify the
/// body concretely instead of inferring Sendness through the self-recursive
/// future type.
fn walk_live(
    client: Arc<UnifiedFileSystem>,
    path: Path,
    semaphore: Arc<Semaphore>,
) -> Pin<Box<dyn Future<Output = FsResult<ContentSummary>> + Send>> {
    Box::pin(async move {
        let status = client.get_status(&path).await?;

        if !status.is_dir {
            // For a file, report this file's own length and its replica factor.
            return Ok(live_file_summary(status.len, status.replicas));
        }

        let children = client.list_status(&path).await?;
        let mut summary = ContentSummary::live_recursive(0, 0, 0, 1);
        let mut dir_tasks = JoinSet::new();

        for child in children {
            let child_path = child_cv_path(&client, &path, Path::from_str(&child.path)?).await?;
            if child.is_dir {
                // The permit moves into the child task: this parent never holds
                // one while awaiting its children, so deep trees cannot starve.
                let permit = Arc::clone(&semaphore)
                    .acquire_owned()
                    .await
                    .map_err(|_| FsError::common("live walk semaphore closed"))?;
                let child_client = Arc::clone(&client);
                let child_semaphore = Arc::clone(&semaphore);
                dir_tasks.spawn(async move {
                    let _permit = permit;
                    walk_live(child_client, child_path, child_semaphore).await
                });
            } else {
                // Each file contributes its own replica factor: multiplying the
                // directory total by one replica count is wrong as soon as the
                // files below it have different factors.
                merge_live_summary(
                    &mut summary,
                    &live_file_summary(child.len, child.replicas),
                );
            }
        }

        while let Some(joined) = dir_tasks.join_next().await {
            let child_summary = joined
                .map_err(|err| FsError::common(format!("live walk task failed: {}", err)))??;
            merge_live_summary(&mut summary, &child_summary);
        }
        Ok(summary)
    })
}

/// Resolves a content summary per the du/count semantics:
///
/// - `snapshot = true`: the dir-space snapshot value (CV namespace only,
///   identical to what the web console shows). Errors surface verbatim —
///   `SnapshotNotReady` means the snapshot has not been published yet and
///   must NOT silently fall back to a live scan.
/// - `snapshot = false`: live recursive walk over the client's configured
///   namespace (unified mode covers CV + UFS data, `--cache-only` is CV-only).
pub async fn resolve_content_summary_for_path(
    client: &UnifiedFileSystem,
    path: &Path,
    snapshot: bool,
) -> FsResult<ContentSummary> {
    resolve_content_summary(
        snapshot,
        || async { client.get_content_summary_cv(path).await },
        || async { calculate_content_summary_live(client, path).await },
    )
    .await
}

/// Routing seam kept generic so tests can assert the mode contract without a
/// live cluster.
async fn resolve_content_summary<S, L>(
    snapshot: bool,
    snapshot_fn: impl FnOnce() -> S,
    live_fn: impl FnOnce() -> L,
) -> FsResult<ContentSummary>
where
    S: Future<Output = FsResult<ContentSummary>>,
    L: Future<Output = FsResult<ContentSummary>>,
{
    if snapshot {
        snapshot_fn().await
    } else {
        live_fn().await
    }
}

pub fn format_content_summary_source(summary: &ContentSummary) -> String {
    summary
        .source
        .as_ref()
        .map(|source| match source {
            ContentSummarySource::Unknown(value) => value.clone(),
            other => other.as_str().to_string(),
        })
        .unwrap_or_else(|| "UNKNOWN".to_string())
}

/// Copy the remaining bytes of `reader` to `out` unchanged and return the
/// number of bytes copied.
///
/// File contents are arbitrary bytes and a multi-byte UTF-8 character can
/// straddle two chunks, so the data must never be decoded on the way through.
/// Only the bytes actually returned by the reader are written, and a reader
/// that ends before its reported length is an error rather than a short copy.
pub(super) async fn copy_to<R: Reader, W: AsyncWrite + Unpin>(
    reader: &mut R,
    out: &mut W,
    chunk_size: usize,
) -> CommonResult<u64> {
    copy_n_to(reader, out, chunk_size, u64::MAX).await
}

/// Like [`copy_to`], but copies at most `limit` bytes from the current
/// position of `reader`. Stops early, without an error, at the end of file.
pub(super) async fn copy_n_to<R: Reader, W: AsyncWrite + Unpin>(
    reader: &mut R,
    out: &mut W,
    chunk_size: usize,
    limit: u64,
) -> CommonResult<u64> {
    let mut buffer = BytesMut::zeroed(chunk_size);
    let mut copied = 0u64;
    let mut left = limit;
    // The reader's length is checked on every pass, as before: a reader that
    // falls back to another source can report a new length after a read.
    while left > 0 && reader.has_remaining() {
        let remaining = reader.remaining() as u64;
        let to_read = (chunk_size as u64).min(remaining).min(left) as usize;
        let read_len = reader.read_full(&mut buffer[..to_read]).await?;
        if read_len == 0 {
            return Err(format!(
                "unexpected end of file at {} of {} bytes",
                reader.pos(),
                reader.len()
            )
            .into());
        }
        out.write_all(&buffer[..read_len]).await?;
        copied += read_len as u64;
        left -= read_len as u64;
    }
    out.flush().await?;
    Ok(copied)
}

/// Formats a size in bytes to a human-readable string (KB, MB, GB, etc.)
pub fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if size >= TB {
        format!("{:.1} TB", size as f64 / TB as f64)
    } else if size >= GB {
        format!("{:.1} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.1} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.1} KB", size as f64 / KB as f64)
    } else {
        format!("{size} B")
    }
}

/// Formats Unix permission bits to a rwx string.
pub fn format_permission(mode: u32) -> String {
    let mode = mode & 0o7777;

    let user_read = if mode & 0o400 != 0 { 'r' } else { '-' };
    let user_write = if mode & 0o200 != 0 { 'w' } else { '-' };
    let user_exec = match (mode & 0o4000 != 0, mode & 0o100 != 0) {
        (true, true) => 's',
        (true, false) => 'S',
        (false, true) => 'x',
        (false, false) => '-',
    };

    let group_read = if mode & 0o040 != 0 { 'r' } else { '-' };
    let group_write = if mode & 0o020 != 0 { 'w' } else { '-' };
    let group_exec = match (mode & 0o2000 != 0, mode & 0o010 != 0) {
        (true, true) => 's',
        (true, false) => 'S',
        (false, true) => 'x',
        (false, false) => '-',
    };

    let other_read = if mode & 0o004 != 0 { 'r' } else { '-' };
    let other_write = if mode & 0o002 != 0 { 'w' } else { '-' };
    let other_exec = match (mode & 0o1000 != 0, mode & 0o001 != 0) {
        (true, true) => 't',
        (true, false) => 'T',
        (false, true) => 'x',
        (false, false) => '-',
    };

    [
        user_read,
        user_write,
        user_exec,
        group_read,
        group_write,
        group_exec,
        other_read,
        other_write,
        other_exec,
    ]
    .iter()
    .collect()
}

/// Formats a Unix epoch timestamp in milliseconds using the local timezone.
pub fn format_epoch_ms_local(timestamp_ms: i64, fmt: &str) -> String {
    if timestamp_ms <= 0 {
        return "-".to_string();
    }

    let Some(datetime) = chrono::DateTime::from_timestamp_millis(timestamp_ms) else {
        return "-".to_string();
    };

    datetime
        .with_timezone(&chrono::Local)
        .format(fmt)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use curvine_error::FsResult;
    use curvine_fs_api::local::LocalReader;
    use curvine_fs_api::state::FileStatus;
    use curvine_fs_api::{Path, Reader};
    use curvine_io::DataSlice;
    use curvine_model::ContentSummarySnapshot;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    /// Reports `len` bytes but serves only `data`, then hits end of file.
    /// With `resized_len`, the reported length changes after the first read,
    /// like a reader that falls back to another source.
    struct TruncatedReader {
        path: Path,
        status: FileStatus,
        data: Option<Bytes>,
        len: i64,
        resized_len: Option<i64>,
        pos: i64,
        chunk: DataSlice,
    }

    impl TruncatedReader {
        fn new(data: &'static [u8], len: i64) -> Self {
            Self {
                path: Path::from_str("/data").unwrap(),
                status: FileStatus::default(),
                data: Some(Bytes::from_static(data)),
                len,
                resized_len: None,
                pos: 0,
                chunk: DataSlice::Empty,
            }
        }

        fn resizing(data: &'static [u8], len: i64, resized_len: i64) -> Self {
            let mut reader = Self::new(data, len);
            reader.resized_len = Some(resized_len);
            reader
        }
    }

    impl Reader for TruncatedReader {
        fn status(&self) -> &FileStatus {
            &self.status
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn len(&self) -> i64 {
            self.len
        }

        fn chunk_mut(&mut self) -> &mut DataSlice {
            &mut self.chunk
        }

        fn chunk_size(&self) -> usize {
            self.len as usize
        }

        fn pos(&self) -> i64 {
            self.pos
        }

        fn pos_mut(&mut self) -> &mut i64 {
            &mut self.pos
        }

        async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
            if let Some(len) = self.resized_len.take() {
                self.len = len;
            }
            Ok(self.data.take().map_or(DataSlice::Empty, DataSlice::bytes))
        }

        async fn seek(&mut self, _pos: i64) -> FsResult<()> {
            unimplemented!()
        }

        async fn complete(&mut self) -> FsResult<()> {
            Ok(())
        }
    }

    struct BrokenPipe;

    impl AsyncWrite for BrokenPipe {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn local_reader(dir: &tempfile::TempDir, content: &[u8], chunk_size: usize) -> LocalReader {
        let file = dir.path().join("data");
        std::fs::write(&file, content).unwrap();

        let path = Path::from_str(file.to_str().unwrap()).unwrap();
        LocalReader::new(&path, chunk_size).unwrap()
    }

    async fn copy_bytes(content: &[u8], reader_chunk: usize, copy_chunk: usize) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, content, reader_chunk);
        let mut out = Vec::new();
        let copied = copy_to(&mut reader, &mut out, copy_chunk).await.unwrap();
        assert_eq!(copied, content.len() as u64);
        out
    }

    #[tokio::test]
    async fn copies_multi_byte_text_split_across_chunks() {
        let content = "中文目录/数据\n".as_bytes();
        assert_eq!(copy_bytes(content, 5, 4).await, content);
    }

    #[tokio::test]
    async fn copies_binary_data_unchanged() {
        let content: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        assert_eq!(copy_bytes(&content, 64, 100).await, content);
    }

    #[tokio::test]
    async fn copies_empty_file() {
        assert!(copy_bytes(b"", 64, 100).await.is_empty());
    }

    #[tokio::test]
    async fn copy_n_to_stops_at_the_limit() {
        // A 250-byte limit with 100-byte copies ends with a partial chunk.
        let dir = tempfile::tempdir().unwrap();
        let content: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let mut reader = local_reader(&dir, &content, 64);
        let mut out = Vec::new();
        let copied = copy_n_to(&mut reader, &mut out, 100, 250).await.unwrap();
        assert_eq!(copied, 250);
        assert_eq!(out, &content[..250]);
    }

    #[tokio::test]
    async fn copy_n_to_stops_at_end_of_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, b"hello", 64);
        let mut out = Vec::new();
        let copied = copy_n_to(&mut reader, &mut out, 100, 4096).await.unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, b"hello");
    }

    #[tokio::test]
    async fn copy_n_to_fails_on_premature_end_of_file_within_the_limit() {
        let mut reader = TruncatedReader::new(b"12345", 10);
        let mut out = Vec::new();
        assert!(copy_n_to(&mut reader, &mut out, 4, 8).await.is_err());
        assert_eq!(out, b"12345");
    }

    #[tokio::test]
    async fn copy_follows_a_length_that_grows_during_the_copy() {
        // Reported as 5 bytes until the first read, then as 10.
        let mut reader = TruncatedReader::resizing(b"0123456789", 5, 10);
        let mut out = Vec::new();
        let copied = copy_to(&mut reader, &mut out, 4).await.unwrap();
        assert_eq!(copied, 10);
        assert_eq!(out, b"0123456789");
    }

    #[tokio::test]
    async fn copy_follows_a_length_that_shrinks_during_the_copy() {
        // Reported as 10 bytes until the first read, then as 5.
        let mut reader = TruncatedReader::resizing(b"0123456789", 10, 5);
        let mut out = Vec::new();
        let copied = copy_to(&mut reader, &mut out, 4).await.unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, b"01234");
    }

    #[tokio::test]
    async fn copy_n_to_keeps_its_limit_when_the_length_grows() {
        let mut reader = TruncatedReader::resizing(b"0123456789", 5, 10);
        let mut out = Vec::new();
        let copied = copy_n_to(&mut reader, &mut out, 4, 7).await.unwrap();
        assert_eq!(copied, 7);
        assert_eq!(out, b"0123456");
    }

    #[tokio::test]
    async fn fails_on_premature_end_of_file() {
        let mut reader = TruncatedReader::new(b"12345", 10);
        let mut out = Vec::new();
        let err = copy_to(&mut reader, &mut out, 4).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("unexpected end of file at 5 of 10 bytes"));
        assert_eq!(out, b"12345");
    }

    #[tokio::test]
    async fn copy_to_file_writes_only_bytes_read() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("download");
        let mut file = tokio::fs::File::create(&target).await.unwrap();
        let mut reader = TruncatedReader::new(b"12345", 10);
        assert!(copy_to(&mut reader, &mut file, 8).await.is_err());
        file.flush().await.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"12345");
    }

    #[tokio::test]
    async fn returns_write_error_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = local_reader(&dir, b"hello", 64);
        assert!(copy_to(&mut reader, &mut BrokenPipe, 100).await.is_err());
    }

    #[test]
    fn test_format_permission() {
        assert_eq!(format_permission(0o755), "rwxr-xr-x");
        assert_eq!(format_permission(0o777), "rwxrwxrwx");
        assert_eq!(format_permission(0o644), "rw-r--r--");
        assert_eq!(format_permission(0o600), "rw-------");
        assert_eq!(format_permission(0o000), "---------");

        assert_eq!(format_permission(0o4755), "rwsr-xr-x");
        assert_eq!(format_permission(0o4644), "rwSr--r--");
        assert_eq!(format_permission(0o2775), "rwxrwsr-x");
        assert_eq!(format_permission(0o2664), "rw-rwSr--");
        assert_eq!(format_permission(0o1755), "rwxr-xr-t");
        assert_eq!(format_permission(0o1666), "rw-rw-rwT");
    }

    #[tokio::test]
    async fn content_summary_snapshot_mode_uses_snapshot_only() {
        static SNAPSHOT_CALLS: AtomicUsize = AtomicUsize::new(0);
        static LIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
        SNAPSHOT_CALLS.store(0, Ordering::SeqCst);
        LIVE_CALLS.store(0, Ordering::SeqCst);

        let summary = resolve_content_summary(
            true,
            || async {
                SNAPSHOT_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_file(42, 1))
            },
            || async {
                LIVE_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_recursive(1, 1, 1, 1))
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.logical_bytes, 42);
        assert_eq!(SNAPSHOT_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(LIVE_CALLS.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn content_summary_default_mode_is_live_only() {
        static SNAPSHOT_CALLS: AtomicUsize = AtomicUsize::new(0);
        static LIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
        SNAPSHOT_CALLS.store(0, Ordering::SeqCst);
        LIVE_CALLS.store(0, Ordering::SeqCst);

        let summary = resolve_content_summary(
            false,
            || async {
                SNAPSHOT_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_file(99, 1))
            },
            || async {
                LIVE_CALLS.fetch_add(1, Ordering::SeqCst);
                Ok(ContentSummary::live_recursive(7, 7, 2, 1))
            },
        )
        .await
        .unwrap();

        assert_eq!(summary.logical_bytes, 7);
        assert_eq!(SNAPSHOT_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(LIVE_CALLS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn content_summary_snapshot_mode_surfaces_not_ready() {
        // No silent fallback: --snapshot must report SnapshotNotReady so the
        // user knows the value is still being scanned.
        let err = resolve_content_summary(
            true,
            || async { Err(FsError::snapshot_not_ready("/a")) },
            || async { Ok(ContentSummary::live_recursive(10, 10, 1, 1)) },
        )
        .await
        .unwrap_err();

        assert!(matches!(err, FsError::SnapshotNotReady(_)));
    }

    #[test]
    fn content_summary_verbose_source_and_generation() {
        let summary = ContentSummary::from_snapshot(
            100,
            300,
            2,
            1,
            ContentSummarySource::Global,
            ContentSummarySnapshot {
                generation: 9,
                started_ms: 1_000,
                finished_ms: 2_000,
                anomaly_count: 0,
                degraded: false,
            },
        );
        assert_eq!(format_content_summary_source(&summary), "GLOBAL");
        assert_eq!(summary.snapshot.as_ref().unwrap().generation, 9);
    }
}
