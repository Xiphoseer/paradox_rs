use structopt::StructOpt;

pub mod util;
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
