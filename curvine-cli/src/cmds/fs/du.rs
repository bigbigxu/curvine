use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::CurvineURI;
use curvine_model::ContentSummary;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum DuCommand {
    /// Calculate directory space usage
    ///
    /// Default: live recursive walk. Without `--cache-only` the unified client
    /// measures CV + UFS data (cache-mode mounts are measured through their
    /// UFS storage); with `--cache-only` only the Curvine namespace is
    /// measured.
    Du {
        #[clap(
            required = true,
            num_args = 1..,
            help = "Paths of the files or directories to calculate"
        )]
        paths: Vec<String>,

        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,

        #[clap(short, long, help = "Display the names of columns as a header line")]
        verbose: bool,

        #[clap(
            long,
            requires = "cache_only",
            help = "Report the dir-space snapshot value (Curvine namespace only, same as the web console). Requires --cache-only"
        )]
        snapshot: bool,

        #[clap(
            long,
            help = "Measure only the Curvine namespace; cache-mode mounts are not delegated to UFS"
        )]
        cache_only: bool,

        #[clap(long, action = clap::ArgAction::Help, help = "Print help")]
        help: Option<bool>,
    },
}

impl DuCommand {
    pub async fn execute(&self, mut client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            DuCommand::Du {
                paths,
                human_readable,
                verbose,
                snapshot,
                cache_only,
                ..
            } => {
                if *cache_only {
                    client.disable_unified();
                }
                if *snapshot && client.unified_enabled() {
                    return Err(
                        "--snapshot measures the Curvine namespace only; add --cache-only (e.g. `cv fs --cache-only du --snapshot <path>`)"
                            .into(),
                    );
                }

                // The header describes the columns, not the path, so it is
                // printed once however many paths follow.
                if *verbose {
                    println!(
                        "{:>15}  {:>15}  {:>15}  PATH",
                        "SIZE", "SINGLE_REPLICA", "ALL_REPLICAS"
                    );
                }

                // One failing path must not hide the sizes of the others, so the
                // error is reported per path and the command fails at the end.
                let mut failed = 0usize;
                for raw in paths {
                    let path = match CurvineURI::new(raw) {
                        Ok(path) => path,
                        Err(e) => {
                            eprintln!("{}", failure_line(raw, &e));
                            failed += 1;
                            continue;
                        }
                    };
                    match crate::cmds::fs::common::resolve_content_summary_for_path(
                        &client,
                        &path,
                        *snapshot,
                    )
                    .await
                    {
                        Ok(summary) => {
                            if *verbose {
                                let source = crate::cmds::fs::common::format_content_summary_source(
                                    &summary,
                                );
                                let (generation, finished) = summary
                                    .snapshot
                                    .as_ref()
                                    .map(|snap| {
                                        (
                                            snap.generation.to_string(),
                                            crate::cmds::fs::common::format_epoch_ms_local(
                                                snap.finished_ms,
                                                "%Y-%m-%d %H:%M:%S",
                                            ),
                                        )
                                    })
                                    .unwrap_or_else(|| ("-".to_string(), "-".to_string()));
                                eprintln!(
                                    "du: source={source} generation={generation} finished={finished}"
                                );
                            }

                            println!(
                                "{}",
                                format_du_row(&summary, path.full_path(), *human_readable)
                            );
                        }
                        Err(FsError::SnapshotNotReady(_)) => {
                            eprintln!(
                                "du: {}: dir-space snapshot is not ready yet (the directory is being scanned); retry after the next scan round, or drop --snapshot for a live scan",
                                path.full_path()
                            );
                            failed += 1;
                        }
                        Err(e) => {
                            eprintln!("{}", failure_line(path.full_path(), &e));
                            failed += 1;
                        }
                    }
                }

                match batch_error(failed, paths.len()) {
                    Some(message) => Err(message.into()),
                    None => Ok(()),
                }
            }
        }
    }
}

/// The stderr line for one path that could not be measured.
///
/// One line per operand, in the order the caller supplied them, so a script can
/// pair the failures with its inputs.
fn failure_line(path: &str, error: &impl std::fmt::Display) -> String {
    format!("du: {}: {}", path, error)
}

/// The closing error, or `None` when every path succeeded.
///
/// Extracted from the command so the count and the exit condition are testable
/// without a cluster.
fn batch_error(failed: usize, total: usize) -> Option<String> {
    (failed > 0).then(|| format!("{} of {} paths failed", failed, total))
}

/// Formats one `du` output row: size, single replica size, size of all replicas, path.
fn format_du_row(summary: &ContentSummary, path: &str, human_readable: bool) -> String {
    // Format similar to HDFS du output with single replica data added
    if human_readable {
        format!(
            "{:>15}  {:>15}  {:>15}  {}",
            crate::cmds::fs::common::format_size(summary.logical_bytes),
            crate::cmds::fs::common::format_size(summary.logical_bytes),
            crate::cmds::fs::common::format_size(summary.replicated_bytes),
            path
        )
    } else {
        format!(
            "{:>15}  {:>15}  {:>15}  {}",
            summary.logical_bytes, summary.logical_bytes, summary.replicated_bytes, path
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{batch_error, format_du_row};

    use curvine_model::{ContentSummary, ContentSummarySnapshot, ContentSummarySource};

    fn live_recursive_dir(files: &[(u64, i32)]) -> ContentSummary {
        let mut summary = ContentSummary::live_recursive(0, 0, 0, 1);
        for &(len, replicas) in files {
            summary.logical_bytes += len;
            summary.replicated_bytes += ContentSummary::replicated_size(len, replicas);
            summary.file_count += 1;
        }
        summary
    }

    #[test]
    fn dir_row_reports_replicated_size_of_files() {
        let row = format_du_row(&live_recursive_dir(&[(1024, 1), (512, 3)]), "/du", false);
        let expected = format!("{:>15}  {:>15}  {:>15}  /du", 1536, 1536, 2560);
        assert_eq!(row, expected);
    }

    #[test]
    fn dir_row_human_readable_reports_replicated_size_of_files() {
        let row = format_du_row(&live_recursive_dir(&[(1024, 1), (512, 3)]), "/du", true);
        let expected = format!("{:>15}  {:>15}  {:>15}  /du", "1.5 KB", "1.5 KB", "2.5 KB");
        assert_eq!(row, expected);
    }

    #[test]
    fn snapshot_row_reports_master_replicated_bytes() {
        let summary = ContentSummary::from_snapshot(
            1024,
            3072,
            4,
            2,
            ContentSummarySource::Global,
            ContentSummarySnapshot {
                generation: 3,
                ..Default::default()
            },
        );

        let row = format_du_row(&summary, "/snap", false);
        assert_eq!(
            row,
            format!("{:>15}  {:>15}  {:>15}  /snap", 1024, 1024, 3072)
        );
    }

    #[test]
    fn batch_error_is_none_only_when_every_path_succeeded() {
        assert_eq!(batch_error(0, 3), None);
        assert_eq!(
            batch_error(1, 3),
            Some("1 of 3 paths failed".to_string())
        );
    }
}
