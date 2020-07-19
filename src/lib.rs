//! # Paradox - LU toolkit
//!
//! This is the `paradox` CLI toolkit exposed as a library. It is a collection of scripts which
//! may be used to read, convert or manipulate data from the defunct game "LEGO (R) Universe".
//!
//! ## Changelog
//!
//! ### 2020-03-30
//! - `wrench`: Completed JSON export of scripts (LUA files)
//!
//! ## TODO
//!
//! - Add maps to `paradox wrench`
//! - Add objects and behaviors to `wrench` or update the explorer
//! - Implement `refs`
use structopt::StructOpt;

pub mod vault;
pub mod wrench;

#[derive(StructOpt)]
/// Does awesome things
pub enum Opt {
    /// Retrieves data from the internet archive
    Vault(self::vault::VaultOpt),
    /// Extracts data from the game resources
    Wrench(self::wrench::WrenchOpt),
}
