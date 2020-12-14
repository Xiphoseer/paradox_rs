use async_std::fs::File;
use futures::{
    io::{AsyncBufReadExt, BufReader},
    stream::StreamExt,
};
use http::Uri;
use spinners::{Spinner, Spinners};
use std::{
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
};
use structopt::StructOpt;
use surf::Client;
use wayback_urls::timemap::{Field, FilterBuf, GroupField, Request};

#[derive(StructOpt)]
pub struct VaultOpt {
    /// Sets the prefix to download from
    pub prefix: Uri,

    /// Sets the output folder to use
    pub output: PathBuf,

    /// Only display the URLs that would be downloaded
    #[structopt(short, long)]
    pub dry_run: bool,

    /// Store only the differing suffix parts
    #[structopt(short, long)]
    pub suffix: bool,

    /// Filter by `[!]field:regex`
    #[structopt(short = "t", long)]
    pub filter: Vec<FilterBuf>,

    /// Don't put directories within the output folder
    #[structopt(short, long)]
    pub flatten: bool,

    /// Collapse the result set on this field
    #[structopt(short, long)]
    pub collapse: Option<Field>,

    #[structopt(short, parse(from_occurrences))]
    /// Split the output path into buckets
    pub buckets: u8,
}

fn make_urlkey(uri: &Uri) -> String {
    let mut iter = uri.host().unwrap().rsplit('.');
    let tld = iter.next().unwrap(); // always valid
    let mut out = String::from(tld);
    for sub in iter {
        out.push(',');
        out.push_str(sub);
    }
    out.push(')');
    out.push_str(uri.path_and_query().unwrap().as_str());
    out
}

pub fn run(opt: &VaultOpt) -> Result<(), anyhow::Error> {
    let prefix = format!("{}", opt.prefix);
    let prefix_key = make_urlkey(&opt.prefix);
    let prefix_len = prefix_key.len();
    println!("{}", prefix_key);

    let mut builder = Request::builder(&prefix)
        .match_prefix()
        .filter(Field::StatusCode, "200");

    for filter in &opt.filter {
        builder = builder.with_filter(filter.as_ref());
    }

    let request = if let Some(c) = opt.collapse {
        builder
            .collapse(c)
            .with_field(GroupField::EndTimestamp)
            .with_field(Field::Original)
            .with_field(Field::UrlKey)
            .done()
    } else {
        builder
            .with_field(Field::Timestamp)
            .with_field(Field::Original)
            .with_field(Field::UrlKey)
            .done()
    };

    let url = request.to_url();
    println!("{}", url);
    let client = Client::new();
    //let client = Client::builder().timeout(None).build().unwrap();

    async_std::task::block_on(async move {
        let spinner = if opt.dry_run {
            None
        } else {
            Some(Spinner::new(
                Spinners::BouncingBar,
                "Downloading...".to_string(),
            ))
        };
        let spinner_ref = &spinner;

        let response = client.get(&url).await.unwrap();
        let buf_reader = BufReader::new(response);

        let client_ref = &client;

        let count: usize = buf_reader
            .lines()
            .map(|x| x.unwrap())
            .then(|x| {
                async move {
                    let mut it = x.split_whitespace();
                    let time = it.next().unwrap();
                    let url = it.next().unwrap();
                    let key = it.next().unwrap();

                    let mut url_parts = key.splitn(2, '?');
                    let url_path = url_parts.next().unwrap();

                    let url_suffix = &key[prefix_len..];

                    // Choose output path
                    let path = if opt.suffix {
                        Path::new(url_suffix)
                    } else {
                        Path::new(url_path)
                    };

                    let ext = path.extension().unwrap_or_else(|| OsStr::new("html"));

                    let parent = path.parent();
                    let stem = path.file_stem().unwrap();

                    let mut name = OsString::new();
                    let mut num_buckets = opt.buckets;

                    let mut store = opt.output.clone();
                    if let Some(p) = parent {
                        let mut iter = p.to_str().unwrap().chars();
                        while num_buckets > 0 {
                            if let Some(c) = iter.next() {
                                let mut buf = [0; 4];
                                store.push(c.encode_utf8(&mut buf));
                            } else {
                                break;
                            }
                            num_buckets -= 1;
                        }
                        if opt.flatten {
                            for component in p.components() {
                                if let Component::Normal(c) = component {
                                    name.push(c);
                                    name.push("_");
                                }
                            }
                        } else {
                            store.push(p);
                        }
                    }

                    if !opt.dry_run {
                        // create the directory
                        async_std::fs::create_dir_all(&store).await.unwrap();
                    }

                    name.push(stem);
                    name.push("_");
                    name.push(time);
                    name.push(".");
                    name.push(ext);

                    store.push(name);

                    let archive_url = format!("https://web.archive.org/web/{}w_/{}", time, url);

                    if opt.dry_run {
                        println!("{} {}", time, url_suffix);
                        println!("=> {}", store.display());
                    } else {
                        let res = client_ref.get(&archive_url).await.unwrap();
                        let mut file = File::create(&store).await.unwrap();
                        futures::io::copy_buf(res, &mut file).await.unwrap();
                    }
                }
            })
            .fold(0, |acc, _x| async move {
                if let Some(sr) = spinner_ref {
                    sr.message(format!("Downloading ... ({})", acc));
                }
                acc + 1
            })
            .await;

        if let Some(s) = spinner {
            s.stop();
        }

        //let text = client.get(&url).send().unwrap().text().unwrap();
        println!("Total: {}", count);
    });
    Ok(())
}
