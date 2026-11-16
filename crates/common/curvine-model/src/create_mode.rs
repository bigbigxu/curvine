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

use crate::CreateCallerCreds;
use curvine_sys as sys;

pub struct CreateMode;

impl CreateMode {
    pub const SETGID: u32 = 0o2000;
    const GROUP_EXEC: u32 = 0o0010;
    const MAX_CALLER_GIDS: usize = 65536;

    pub fn resolve_group_gid(group: &str) -> Option<u32> {
        group
            .parse::<u32>()
            .ok()
            .or_else(|| sys::get_gid_by_name(group))
    }

    pub fn created_group_gid(
        parent_mode: u32,
        parent_group: &str,
        effective_gid: u32,
    ) -> Option<u32> {
        if parent_mode & Self::SETGID != 0 {
            Self::resolve_group_gid(parent_group)
        } else {
            Some(effective_gid)
        }
    }

    pub fn caller_in_created_group(
        creds: &CreateCallerCreds,
        inherited_group: Option<&str>,
    ) -> bool {
        match inherited_group {
            None => true,
            Some(group) => {
                creds.parent_group.as_deref() == Some(group)
                    && creds.parent_group_gid.is_some_and(|gid| {
                        creds.gids.len() <= Self::MAX_CALLER_GIDS && creds.gids.contains(&gid)
                    })
            }
        }
    }

    pub fn normalize_create_mode(
        requested_mode: u32,
        umask: u32,
        caller_in_created_group: bool,
        has_cap_fsetid: bool,
    ) -> u32 {
        let requested_mode = requested_mode & 0o7777;
        let mut mode = requested_mode & !(umask & 0o777);
        let requested_setgid_exec =
            requested_mode & (Self::SETGID | Self::GROUP_EXEC) == (Self::SETGID | Self::GROUP_EXEC);

        if requested_setgid_exec && !caller_in_created_group && !has_cap_fsetid {
            mode &= !Self::SETGID;
        }
        mode
    }
}

#[cfg(test)]
mod tests {
    use super::CreateMode;
    use crate::CreateCallerCreds;

    #[test]
    fn strips_setgid_for_unprivileged_non_member() {
        assert_eq!(
            CreateMode::normalize_create_mode(0o2010, 0, false, false),
            0o2010 & !0o2000
        );
    }

    #[test]
    fn checks_group_exec_before_applying_umask() {
        // umask removes the group-exec bit, but authorization is decided on the
        // requested mode, so setgid is still stripped.
        assert_eq!(
            CreateMode::normalize_create_mode(0o2010, 0o0010, false, false),
            0o2000 & !0o2000
        );
    }

    #[test]
    fn preserves_setgid_for_group_member_or_capability() {
        assert_eq!(
            CreateMode::normalize_create_mode(0o2010, 0, true, false),
            0o2010
        );
        assert_eq!(
            CreateMode::normalize_create_mode(0o2010, 0, false, true),
            0o2010
        );
    }

    #[test]
    fn preserves_non_executable_setgid() {
        assert_eq!(
            CreateMode::normalize_create_mode(0o2000, 0, false, false),
            0o2000
        );
    }

    #[test]
    fn applies_umask_and_strips_file_type_bits() {
        assert_eq!(
            CreateMode::normalize_create_mode(0o100644, 0o022, false, false),
            0o644
        );
    }

    #[test]
    fn resolve_group_gid_has_no_configuration_fallback() {
        assert_eq!(CreateMode::resolve_group_gid("12345"), Some(12345));
        assert_eq!(
            CreateMode::resolve_group_gid("curvine-review-group-that-does-not-exist"),
            None
        );
    }

    #[test]
    fn created_group_gid_follows_parent_setgid() {
        assert_eq!(
            CreateMode::created_group_gid(0o755, "curvine-review-group-that-does-not-exist", 7),
            Some(7)
        );
        assert_eq!(CreateMode::created_group_gid(0o2755, "4242", 7), Some(4242));
        assert_eq!(
            CreateMode::created_group_gid(0o2755, "curvine-review-group-that-does-not-exist", 7),
            None
        );
    }

    #[test]
    fn caller_in_created_group_requires_reported_gid() {
        let creds =
            |gids: Vec<u32>, parent_group: Option<&str>, parent_group_gid| CreateCallerCreds {
                gids,
                umask: 0,
                cap_fsetid: false,
                parent_group_gid,
                parent_group: parent_group.map(str::to_string),
            };

        assert!(CreateMode::caller_in_created_group(
            &creds(vec![1], None, None),
            None
        ));
        assert!(CreateMode::caller_in_created_group(
            &creds(vec![1, 2], Some("grp"), Some(2)),
            Some("grp")
        ));
        assert!(!CreateMode::caller_in_created_group(
            &creds(vec![1, 2], Some("grp"), Some(3)),
            Some("grp")
        ));
        assert!(!CreateMode::caller_in_created_group(
            &creds(vec![1, 2], Some("grp"), Some(2)),
            Some("other")
        ));
        assert!(!CreateMode::caller_in_created_group(
            &creds(vec![1, 2], None, Some(2)),
            Some("grp")
        ));
        assert!(!CreateMode::caller_in_created_group(
            &creds(vec![1, 2], Some("grp"), None),
            Some("grp")
        ));
        assert!(!CreateMode::caller_in_created_group(
            &creds(vec![1; 65537], Some("grp"), Some(1)),
            Some("grp")
        ));
    }
}
