use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::CurvineURI;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum CountCommand {
    /// Count files and directories
    ///
    /// Default: live recursive walk. Without `--cache-only` the unified client
    /// measures CV + UFS data; with `--cache-only` only the Curvine namespace
    /// is measured.
    Count {
        #[clap(help = "Path of the directory to count")]
        path: String,

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
    },
}

impl CountCommand {
    pub async fn execute(&self, mut client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CountCommand::Count {
                path,
                snapshot,
                cache_only,
            } => {
                if *cache_only {
                    client.disable_unified();
                }
                if *snapshot && client.unified_enabled() {
                    return Err(
                        "--snapshot measures the Curvine namespace only; add --cache-only (e.g. `cv fs count --cache-only --snapshot <path>`)"
                            .into(),
                    );
                }
                let path = CurvineURI::new(path)?;

                match crate::cmds::fs::common::resolve_content_summary_for_path(
                    &client,
                    &path,
                    *snapshot,
                )
                .await
                {
                    Ok(summary) => {
                        println!("   DIR_COUNT    FILE_COUNT       CONTENT_SIZE PATHNAME");
                        println!(
                            "{:>12} {:>12} {:>18} {}",
                            summary.directory_count,
                            summary.file_count,
                            summary.logical_bytes,
                            path.full_path()
                        );
                        Ok(())
                    }
                    Err(FsError::SnapshotNotReady(_)) => {
                        Err(format!(
                            "Cannot count '{}': dir-space snapshot is not ready yet (the directory is being scanned); retry after the next scan round, or drop --snapshot for a live scan",
                            path.full_path()
                        )
                        .into())
                    }
                    Err(e) => Err(format!("Cannot count '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
