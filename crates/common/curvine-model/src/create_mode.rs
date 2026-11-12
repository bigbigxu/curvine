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

//! Shared file-creation mode normalization.
//!
//! The same rules are applied by the FUSE client (when it must normalize
//! locally) and by the master (when the client sends raw mode plus caller
//! credentials), so the two sides can never drift.

use curvine_sys as sys;

pub const MODE_SETGID: u32 = 0o2000;
const MODE_GROUP_EXEC: u32 = 0o0010;

/// Resolve a stored numeric GID or group name without a configuration fallback.
pub fn resolve_group_gid(group: &str) -> Option<u32> {
    group
        .parse::<u32>()
        .ok()
        .or_else(|| sys::get_gid_by_name(group))
}

/// Apply Linux file-creation ordering: decide whether setgid is authorized from
/// the requested mode, then apply the request umask to the persisted mode.
pub fn normalize_create_mode(
    requested_mode: u32,
    umask: u32,
    caller_in_created_group: bool,
    has_cap_fsetid: bool,
) -> u32 {
    let requested_mode = requested_mode & 0o7777;
    let mut mode = requested_mode & !umask;
    let requested_setgid_exec =
        requested_mode & (MODE_SETGID | MODE_GROUP_EXEC) == (MODE_SETGID | MODE_GROUP_EXEC);

    if requested_setgid_exec && !caller_in_created_group && !has_cap_fsetid {
        mode &= !MODE_SETGID;
    }
    mode
}

#[cfg(test)]
mod tests {
    use super::{normalize_create_mode, resolve_group_gid};

    #[test]
    fn strips_setgid_for_unprivileged_non_member() {
        assert_eq!(
            normalize_create_mode(0o2010, 0, false, false),
            0o2010 & !0o2000
        );
    }

    #[test]
    fn checks_group_exec_before_applying_umask() {
        // umask removes the group-exec bit, but authorization is decided on the
        // requested mode, so setgid is still stripped.
        assert_eq!(
            normalize_create_mode(0o2010, 0o0010, false, false),
            0o2000 & !0o2000
        );
    }

    #[test]
    fn preserves_setgid_for_group_member_or_capability() {
        assert_eq!(normalize_create_mode(0o2010, 0, true, false), 0o2010);
        assert_eq!(normalize_create_mode(0o2010, 0, false, true), 0o2010);
    }

    #[test]
    fn preserves_non_executable_setgid() {
        assert_eq!(normalize_create_mode(0o2000, 0, false, false), 0o2000);
    }

    #[test]
    fn applies_umask_and_strips_file_type_bits() {
        assert_eq!(normalize_create_mode(0o100644, 0o022, false, false), 0o644);
    }

    #[test]
    fn resolve_group_gid_has_no_configuration_fallback() {
        assert_eq!(resolve_group_gid("12345"), Some(12345));
        assert_eq!(
            resolve_group_gid("curvine-review-group-that-does-not-exist"),
            None
        );
    }
}
