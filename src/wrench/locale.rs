use {
    super::Res,
    anyhow::anyhow,
    quick_xml::{
        events::{BytesEnd as XmlBytesEnd, BytesStart as XmlBytesStart, Event as XmlEvent},
        Reader as XmlReader,
    },
    std::{error::Error as StdError, io::BufRead, str::FromStr},
};

pub fn expect_start<'a, 'b, 'c, B: BufRead>(
    key: &'a str,
    reader: &'b mut XmlReader<B>,
    buf: &'c mut Vec<u8>,
) -> Res<XmlBytesStart<'c>> {
    if let Ok(XmlEvent::Start(e)) = reader.read_event(buf) {
        if e.name() == key.as_bytes() {
            Ok(e)
        } else {
            Err(anyhow!("Expected tag `{}`, found `{:?}`", key, e.name()))
        }
    } else {
        Err(anyhow!("Missing tag `{}`", key))
    }
}

pub fn expect_end<'a, 'b, 'c, B: BufRead>(
    key: &'a str,
    reader: &'b mut XmlReader<B>,
    buf: &'c mut Vec<u8>,
) -> Res<XmlBytesEnd<'c>> {
    if let Ok(XmlEvent::End(e)) = reader.read_event(buf) {
        if e.name() == key.as_bytes() {
            Ok(e)
        } else {
            Err(anyhow!(
                "Expected end tag `{}`, found `{:?}`",
                key,
                e.name()
            ))
        }
    } else {
        Err(anyhow!("Missing end tag `{}`", key))
    }
}

pub fn expect_text<'b, 'c, B: BufRead>(
    reader: &'b mut XmlReader<B>,
    buf: &'c mut Vec<u8>,
) -> Res<String> {
    if let Ok(XmlEvent::Text(e)) = reader.read_event(buf) {
        let text = e.unescape_and_decode(&reader)?;
        Ok(text)
    } else {
        Err(anyhow!("Missing text"))
    }
}

pub fn expect_attribute<T: FromStr, B: BufRead>(
    key: &str,
    reader: &XmlReader<B>,
    event: &XmlBytesStart,
) -> Res<T>
where
    <T as FromStr>::Err: StdError + Send + Sync + 'static,
{
    let attr = event
        .attributes()
        .next()
        .ok_or_else(|| anyhow!("Missing Attribute `{}`", key))??;

    if attr.key == key.as_bytes() {
        let attr_unesc = attr.unescaped_value()?;
        let attr_str = reader.decode(&attr_unesc)?;
        let value = attr_str.parse()?;
        Ok(value)
    } else {
        Err(anyhow!(
            "Expected Attribute `{}`, found `{:?}`",
            key,
            attr.key
        ))
    }
}
