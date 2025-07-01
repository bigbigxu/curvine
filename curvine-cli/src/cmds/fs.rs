// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::util::bytes_to_string;
use clap::{Parser, Subcommand};
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::{CurvineURI, Writer};
use num_bigint::BigInt;
use orpc::CommonResult;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

#[derive(Parser, Debug)]
pub struct FsCommand {
    #[clap(subcommand)]
    action: FsSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum FsSubCommand {
    /// List files and directories
    Ls {
        #[clap(value_name = "path", default_value = "/")]
        path: String,
    },

    /// Create a directory
    Mkdir {
        #[clap(help = "Directory path to create")]
        path: String,

        #[clap(short, long, help = "Create parent directories as needed")]
        parents: bool,
    },

    /// Upload a file to the filesystem
    Put {
        #[clap(help = "Local file path")]
        local_path: PathBuf,

        #[clap(help = "Remote destination path")]
        remote_path: String,
    },

    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,
    },

    /// Show disk usage statistics
    Df {
        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,
    },

    /// Create an empty file or update timestamp
    Touch {
        #[clap(help = "Path of the file to create or touch")]
        path: String,
    },
}

impl FsCommand {
    pub async fn execute(&self, client: CurvineFileSystem) -> CommonResult<()> {
        match &self.action {
            FsSubCommand::Ls { path } => {
                let path = CurvineURI::new(path)?;
                println!("List Path {}", path.full_path());
                let files = client.list_status(&path).await?;

                if files.is_empty() {
                    return Ok(());
                }

                // Print header similar to HDFS ls output
                println!("Found {} items", files.len());

                for file in files {
                    // Format similar to HDFS: permissions owner group size date time name
                    let file_type = if file.is_dir { "d" } else { "-" };
                    let permissions = "rwxr-xr-x"; // Default permissions for now
                    let replicas = if file.is_dir {
                        "-"
                    } else {
                        &file.replicas.to_string()
                    };
                    let owner = "curvine"; // Default owner
                    let group = "curvine"; // Default group

                    // Format file size
                    let size = if file.is_dir {
                        "-".to_string()
                    } else {
                        file.len.to_string()
                    };

                    // Format modification time (convert from timestamp)
                    let datetime = chrono::DateTime::from_timestamp(file.mtime / 1000, 0)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    let formatted_time = datetime.format("%Y-%m-%d %H:%M").to_string();

                    // Print in HDFS-like format
                    println!(
                        "{}{} {} {} {} {:>12} {} {}",
                        file_type,
                        permissions,
                        replicas,
                        owner,
                        group,
                        size,
                        formatted_time,
                        file.name
                    );
                }
                Ok(())
            }

            FsSubCommand::Mkdir { path, parents } => {
                println!("Creating directory: {} (parents: {})", path, parents);
                let path = CurvineURI::new(path)?;
                client.mkdir(&path, *parents).await?;
                println!("Directory created successfully");
                Ok(())
            }

            FsSubCommand::Put {
                local_path,
                remote_path,
            } => {
                // Check if local file exists
                if !local_path.exists() {
                    eprintln!(
                        "Error: Local file '{}' does not exist",
                        local_path.display()
                    );
                    return Ok(());
                }

                // Check if local path is a file (not directory)
                if !local_path.is_file() {
                    eprintln!("Error: '{}' is not a file", local_path.display());
                    return Ok(());
                }

                // Get file size for progress tracking
                let file_size = match local_path.metadata() {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        eprintln!(
                            "Error getting file metadata '{}': {}",
                            local_path.display(),
                            e
                        );
                        return Ok(());
                    }
                };

                println!(
                    "Uploading {} ({} bytes) to {}",
                    local_path.display(),
                    file_size,
                    remote_path
                );

                // Open local file for streaming read
                let file = match fs::File::open(local_path).await {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("Error opening local file '{}': {}", local_path.display(), e);
                        return Ok(());
                    }
                };
                let mut reader = BufReader::new(file);

                // Determine the final remote path
                let final_remote_path = if remote_path.ends_with('/') {
                    // If remote path ends with '/', it's definitely a directory
                    let local_filename = local_path.file_name().unwrap().to_string_lossy();
                    format!("{}{}", remote_path, local_filename)
                } else {
                    // Check if remote path exists and is a directory
                    match CurvineURI::new(remote_path) {
                        Ok(remote_check_uri) => {
                            match client.get_status(&remote_check_uri).await {
                                Ok(status) if status.is_dir => {
                                    // Remote path exists and is a directory
                                    let local_filename =
                                        local_path.file_name().unwrap().to_string_lossy();
                                    format!("{}/{}", remote_path, local_filename)
                                }
                                _ => {
                                    // Remote path doesn't exist or is a file, use as-is
                                    remote_path.to_string()
                                }
                            }
                        }
                        Err(_) => {
                            // If URI parsing fails, treat as file path
                            remote_path.to_string()
                        }
                    }
                };

                // Create remote path URI
                let remote_uri = match CurvineURI::new(&final_remote_path) {
                    Ok(uri) => uri,
                    Err(e) => {
                        eprintln!("Error parsing remote path '{}': {}", final_remote_path, e);
                        return Ok(());
                    }
                };

                // Create writer for streaming upload
                match client.create(&remote_uri, true).await {
                    Ok(mut writer) => {
                        let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
                        let mut total_bytes = 0u64;
                        let mut last_progress = 0u64;

                        loop {
                            match reader.read(&mut buffer).await {
                                Ok(0) => break, // EOF
                                Ok(bytes_read) => {
                                    // Write chunk to curvine
                                    match writer.write(&buffer[..bytes_read]).await {
                                        Ok(_) => {
                                            total_bytes += bytes_read as u64;

                                            // Show progress every 10MB or at completion
                                            if total_bytes - last_progress >= 10 * 1024 * 1024
                                                || total_bytes == file_size
                                            {
                                                let progress =
                                                    (total_bytes as f64 / file_size as f64 * 100.0)
                                                        as u32;
                                                println!(
                                                    "Progress: {}% ({}/{} bytes)",
                                                    progress, total_bytes, file_size
                                                );
                                                last_progress = total_bytes;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Error writing chunk: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error reading from local file: {}", e);
                                    return Ok(());
                                }
                            }
                        }

                        // Complete the upload
                        match writer.complete().await {
                            Ok(_) => {
                                println!(
                                    "Successfully uploaded {} bytes to {}",
                                    total_bytes, final_remote_path
                                );
                            }
                            Err(e) => {
                                eprintln!("Error completing upload: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error creating remote file '{}': {}", final_remote_path, e);
                    }
                }

                Ok(())
            }

            FsSubCommand::Cat { path } => {
                let path = CurvineURI::new(path)?;

                match client.read_string(&path.clone()).await {
                    Ok(content) => {
                        print!("{}", content);
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("cat: {}: {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }

            FsSubCommand::Df { human_readable } => match client.get_master_info().await {
                Ok(info) => {
                    if *human_readable {
                        println!("Filesystem            Size   Used  Available  Use%");
                        println!(
                            "curvine      {:>10} {:>6} {:>9} {:>4}",
                            bytes_to_string(&BigInt::from(info.capacity)),
                            bytes_to_string(&BigInt::from(info.fs_used)),
                            bytes_to_string(&BigInt::from(info.available)),
                            if info.capacity > 0 {
                                format!("{}%", (info.fs_used * 100) / info.capacity)
                            } else {
                                "0%".to_string()
                            }
                        );
                    } else {
                        println!("Filesystem            Size   Used  Available  Use%");
                        println!(
                            "curvine      {:>10} {:>6} {:>9} {:>4}",
                            info.capacity,
                            info.fs_used,
                            info.available,
                            if info.capacity > 0 {
                                format!("{}%", (info.fs_used * 100) / info.capacity)
                            } else {
                                "0%".to_string()
                            }
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Error getting filesystem info: {}", e);
                    Err(e.into())
                }
            },

            FsSubCommand::Touch { path } => {
                let path = CurvineURI::new(path)?;
                println!("Touching file: {}", path.full_path());

                // Try to create an empty file
                match client.create(&path.clone(), false).await {
                    Ok(mut writer) => {
                        // Write empty content and complete
                        match writer.complete().await {
                            Ok(_) => {
                                println!("File created: {}", path.full_path());
                                Ok(())
                            }
                            Err(e) => {
                                eprintln!(
                                    "Error completing file creation {}: {}",
                                    path.full_path(),
                                    e
                                );
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        // If file already exists, that's fine for touch command
                        if e.to_string().contains("already exists") {
                            println!("File already exists: {}", path.full_path());
                            Ok(())
                        } else {
                            eprintln!("Error creating file {}: {}", path.full_path(), e);
                            Err(e.into())
                        }
                    }
                }
            }
        }
    }
}
