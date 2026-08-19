fn main() {
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    println!("cargo:rustc-check-cfg=cfg(redb_no_std)");
    println!("cargo:rustc-check-cfg=cfg(redb_branch_variable_width_offsets)");
    println!("cargo:rustc-check-cfg=cfg(redb_branch_checksum_pages)");
    println!("cargo:rerun-if-env-changed=REDB_BRANCH_VARIABLE_WIDTH_OFFSETS");
    println!("cargo:rerun-if-env-changed=REDB_BRANCH_CHECKSUM_PAGES");

    // Opt-in, format-breaking prototypes used by the branch-density benchmark. Environment
    // switches keep them out of `--all-features`, which must continue to exercise the stable
    // format and backward-compatibility tests.
    if std::env::var_os("REDB_BRANCH_VARIABLE_WIDTH_OFFSETS").is_some() {
        println!("cargo:rustc-cfg=redb_branch_variable_width_offsets");
    }
    if std::env::var_os("REDB_BRANCH_CHECKSUM_PAGES").is_some() {
        println!("cargo:rustc-cfg=redb_branch_checksum_pages");
    }

    // Building without the standard library is only offered under the redb 5 API preview. Cargo
    // features cannot express "experimental-api-5 and not std", so it is computed here and used as
    // `cfg(redb_no_std)` throughout the crate. Turning "std" off without "experimental-api-5" keeps
    // the std build, so that 4.x dependents who set `default-features = false` are unaffected.
    if std::env::var_os("CARGO_FEATURE_EXPERIMENTAL_API_5").is_some()
        && std::env::var_os("CARGO_FEATURE_STD").is_none()
    {
        println!("cargo:rustc-cfg=redb_no_std");
    }

    if std::env::var("CARGO_CFG_FUZZING").is_ok()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
