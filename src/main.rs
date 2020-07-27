use paradox::{vault, wrench::WrenchState, Opt};
use structopt::StructOpt;

fn main() -> Result<(), anyhow::Error> {
    let opt = Opt::from_args();

    match opt {
        Opt::Vault(v) => {
            vault::run(&v)?;
            Ok(())
        }
        Opt::Wrench(w) => WrenchState::new(w).run(),
    }
}
