use paradox::{vault, Opt};
use structopt::StructOpt;

fn main() -> Result<(), anyhow::Error> {
    let opt = Opt::from_args();

    match opt {
        Opt::Vault(v) => {
            vault::run(&v)?;
            Ok(())
        }
        Opt::Wrench(w) => w.run(),
    }
}
