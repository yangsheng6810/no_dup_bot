use teloxide::{prelude::*, net::Download, types::File as TgFile, types::PhotoSize};
use tokio::fs::File;

use std::sync::Arc;
use tokio::sync::Mutex;

use url::Url;
use serde::{Deserialize, Serialize};

use std::time::{SystemTime, UNIX_EPOCH};
use std::io::{Error, ErrorKind};

use anyhow::Result;
use img_hash::ImageHash;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageInfo {
    #[serde(with = "url_serde")]
    url: Url,
    count: u32,
    #[serde(with = "url_serde")]
    link: Option<Url>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct MessageKey {
    chat_id: String,
    #[serde(with = "url_serde")]
    url: Url
}

impl PartialEq for MessageKey {
    fn eq(&self, other: &Self) -> bool {
        self.chat_id.eq(&other.chat_id) && self.url.eq(&other.url)
    }
}

impl Eq for MessageKey {}

pub trait KVStore {
    fn init(file_path: &str) -> Self;
    fn save(&self, k: &MessageKey, v: &MessageInfo) -> bool;
    fn find(&self, k: &MessageKey) -> Option<MessageInfo>;
    fn delete(&self, k: &MessageKey) -> bool;
}

pub struct MyDB {
    db: sled::Db,
}

impl KVStore for MyDB {
    fn init(file_path: &str) -> Self {
        MyDB { db: sled::open(file_path).unwrap()}
    }

    fn save(&self, k: &MessageKey, v: &MessageInfo) -> bool {
        let serialized_k = serde_json::to_string(&k).unwrap();
        let serialized_v = serde_json::to_string(&v).unwrap();

        if self.db.insert(serialized_k.as_bytes(), serialized_v.as_bytes()).is_err() {
            println!("database seve error when saving key {:?} with value {:?}", &k, &v);
            false
        } else {
            true
        }
    }

    fn find(&self, k: &MessageKey) -> Option<MessageInfo> {
        let serialized_k = serde_json::to_string(&k).unwrap();
        match self.db.get(serialized_k.as_bytes()) {
            Ok(Some(v)) => {
                let result = String::from_utf8(v.to_vec()).unwrap();
                println!("Finding '{:?}' returns '{}'", k, result);
                let result: MessageInfo = serde_json::from_str(&result).unwrap();
                Some(result)
            },
            Ok(None) => {
                println!("Finding '{:?}' returns None", k);
                None
            },
            Err(e) => {
                println!("Error retrieving value for {:?}: {}", k, e);
                None
            }
        }
    }

    fn delete(&self, k: &MessageKey) -> bool {
        let serialized_k = serde_json::to_string(&k).unwrap();
        self.db.remove(serialized_k.as_bytes()).is_ok()
    }
}

fn get_chat_id(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> String {
    let id = ctx.update.chat_id();
    let id_str = id.to_string();
    id_str.strip_prefix("-100")
          .map_or(id_str.clone(),
                  |id| String::from(id))
}

fn get_msg_link(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    if ctx.update.chat.is_private() {
        return None;
    }
    let id = ctx.update.id;
    let url = match ctx.update.chat.username() {
            // If it's public group (i.e. not DM, not private group), we can produce
            // "normal" t.me link (accesible to everyone).
            Some(username) => format!("https://t.me/{0}/{1}/", username, id),
            // For private groups we produce "private" t.me/c links. These are only
            // accesible to the group members.
            None => format!("https://t.me/c/{0}/{1}/", get_chat_id(&ctx), id),
        };
    Some(Url::parse(&url).unwrap())
}

fn get_forward_msg_link(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    let chat = message.update.forward_from_chat()?;
    dbg!(chat.username());
    if let (Some(username), Some(message_id))
        = (chat.username(), message.update.forward_from_message_id())
    {
        let url = Url::parse(&format!("https://t.me/{}/{}", username, message_id)).ok();
        dbg!(&url);
        url
    } else {
        println!("Parse forwarded message failed");
        dbg!(chat);
        None
    }
}

fn get_url(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    let ss = ctx.update.text().to_owned()?;
    Url::parse(ss).ok()
}

fn get_text(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<String> {
    let ss = ctx.update.text().to_owned()?;
    Some(String::from(ss))
}

fn get_filename() -> String {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went back");
    let filename = format!("./{}.jpeg", duration.as_millis());
    filename
}

fn remove_file(filename: &str) {
    std::fs::remove_file(filename)
        .err().map(|e| println!("Unable to delete file {:?}", e));
}

async fn get_hash(ctx: &UpdateWithCx<AutoSend<Bot>, Message>, img_to_download: &PhotoSize) -> Result<Option<String>>{
    let filename = get_filename();
    let TgFile { file_path, .. } = ctx.requester.get_file(&img_to_download.file_id).send().await?;
    match File::create(&filename).await {
        Ok(mut file) => {
            match ctx.requester.download_file(&file_path, &mut file).await {
                Ok(x) => {
                    dbg!(x);
                    println!("Download success to file {:?}", file);
                    file.sync_all().await?;
                    match image::open(&filename) {
                        Ok(image) => {
                            let hasher = img_hash::HasherConfig::new().to_hasher();
                            let hash = Some(hasher.hash_image(&image).to_base64());
                            remove_file(&filename);
                            Ok(hash)
                        },
                        Err(e) => {
                            println!("Failed to re-read file, {:?}", e);
                            // Temporarily disable file remove for debug purpose
                            // remove_file(&filename);
                            Ok(None)
                        }
                    }
                },
                Err(e) => {
                    println!("Download error {:?}", e);
                    remove_file(&filename);
                    Ok(None)
                }
            }
        },
        Err(e) => {
            println!("Cannot create tempfile due to {:?}", e);
            remove_file(&filename);
            Ok(None)
        }
    }
}

// fn sled_to_object<'a, T>(value: sled::IVec) -> T
// where
//     T: Deserialize<'a>
// {
//     serde_json::from_str::<T>(&String::from_utf8(value.to_vec()).unwrap()).unwrap()
// }

#[allow(dead_code)]
fn object_to_sled<T>(ss: T) -> sled::IVec
where
    T: Serialize
{
    sled::IVec::from(serde_json::to_string(&ss).unwrap().as_bytes())
}

async fn insert_img_hash(img_db: Arc<Mutex<sled::Db>>, hash: String, key: MessageKey) -> bool {
    let img_db = img_db.lock().await;

    let serialized_k = serde_json::to_string(&hash).unwrap();
    let serialized_v = serde_json::to_string(&key).unwrap();

    if img_db.insert(serialized_k.as_bytes(), serialized_v.as_bytes()).is_err() {
        println!("database seve error when saving key {:?} with value {:?}", &hash, &key);
        false
    } else {
        true
    }
}

async fn check_img_hash(img_db: Arc<Mutex<sled::Db>>, hash: String) -> Result<Option<MessageKey>> {
    let similarity_threshold = 10u32;
    let hash = ImageHash::from_base64(&hash).unwrap();
    let img_db = img_db.lock().await;
    let mut best_hash: Option<ImageHash> = None;
    let mut best_dist: Option<u32> = None;
    let mut best_url: Option<MessageKey> = None;
    for ans in img_db.iter() {
        ans.ok().map(
            |(key, value)| {
                // let key = sled_to_object::<String>(key);
                // let value = sled_to_object::<MessageKey>(value);

                let key = serde_json::from_str::<String>(
                    &String::from_utf8(key.to_vec()).unwrap()).unwrap();

                let value = serde_json::from_str::<MessageKey>(
                    &String::from_utf8(value.to_vec()).unwrap()).unwrap();

                println!("Saw {:?} {:?}", &key, &value);
                let iter_hash = ImageHash::from_base64(&key).unwrap();
                let dist = iter_hash.dist(&hash);
                match best_dist {
                    None => {
                        best_hash = Some(iter_hash);
                        best_dist = Some(dist);
                        best_url = Some(value);
                    },
                    Some(old_dist) => {
                        if dist < old_dist {
                            best_hash = Some(iter_hash);
                            best_dist = Some(dist);
                            best_url = Some(value);
                        }
                    }
                }
            });
    }
    match best_dist {
        Some(dist) => {
            println!("The best distance is {} among all {} entries", dist, count);
            if dist < similarity_threshold {
                best_hash.map(|h| println!("Use this hash! {:?}", h.to_base64()));
                best_url.as_ref().map(|u| println!("with url {:?}", u));
                Ok(best_url)
            } else {
                Ok(None)
            }
        },
        None => {
            Ok(None)
        }
    }
}

async fn parse_message(
    ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
    db: Arc<Mutex<MyDB>>,
    img_db: Arc<Mutex<sled::Db>>
) -> Result<()> {
    let mut url: Option<Url>;
    let link = get_msg_link(&ctx);
    let chat_id = get_chat_id(&ctx);

    match (is_forward(&ctx), is_image(&ctx)) {
        (true, false) => {
            // is a forward message
            println!("Found a forwarded message");
            url = get_forward_msg_link(&ctx);
            if url.is_none(){
                println!("Forwarded message link parse failure.")
            }
        },
        // (true, true) => {
        //     // is a forward and an image
        //     println!("Found an image message that is also forward");
        //     url = get_forward_msg_link(&ctx);
        //     if url.is_none(){
        //         println!("Forwarded message link parse failure.")
        //     }
        // },
        (_, true) => {
            // is an image, but not forward
            println!("Found an image message that is not a forward");
            url = None;
            let img_vec = ctx.update.photo()
                                    .ok_or(Error::new(ErrorKind::Other, "failed to download img_vec"))?;
            let mut img_to_download: Option<PhotoSize> = None;
            for img in img_vec.iter() {
                // dbg!(img);
                match img_to_download {
                    None => {
                        img_to_download = Some(img.clone());
                    },
                    Some(ref temp_img) => {
                        if img.width <= 600 && img.width > temp_img.width {
                            img_to_download = Some(img.clone());
                        }
                    }
                }
            }
            if let Some(img) = img_to_download {
                match get_hash(&ctx, &img).await {
                    Ok(Some(hash)) => {
                        println!("Get hash {}", &hash);
                        match check_img_hash(img_db.clone(), hash.clone()).await {
                            Ok(Some(key)) => {
                                println!("Found existing hash {:?} that is close", key.url);
                                url = Some(key.url.clone());
                            },
                            _ => {
                                println!("No close hash is found, use original hash {:?}", &hash);
                                url = Url::parse(&format!("https://img.telegram.com/{}", hash)).ok();
                                if let Some(url) = url.clone() {
                                    let key = MessageKey{chat_id: chat_id.clone(), url:url.clone()};
                                    let ans = insert_img_hash(img_db, hash.clone(), key.clone()).await;
                                    if ! ans {
                                        println!("insert error, with hash {:?} and key {:?}", &hash, &key);
                                    }
                                }
                            }
                        }
                    },
                    Ok(None) => {
                        println!("Failed to get hash");
                    }
                    Err(e) => {
                        println!("Get hash error {:?}", e);
                    }
                };
            }
        },
        (false, false) => {
            // not forward nor image, only interested in pure url
            println!("Found a non-forward, non-image message");
            url = get_url(&ctx);
            if url.is_none(){
                println!("Non-forwarded message link parse failure.")
            }
        }
    }
    if let Some(url) = url {
        let key = MessageKey{chat_id, url:url.clone()};
        let db = db.lock().await;
        if let Some(info) = db.find(&key){
            let mut info = info.clone();
            // has seen this message before
            info.count += 1;
            db.save(&key, &info);
            // ctx.answer(format!("See it {} times", info.count)).await?;
            println!("See it {} times", info.count);
            let link_msg = &info.link.map_or(
                format!("第一次出现是在private chat"),
                |url| format!("第一次出现是在：{}", url));
            // ctx.answer(&link_msg).await?;
            let final_msg = format!("你火星了！这条消息是第{}次来到本群了，快去爬楼。{}", info.count, link_msg);
            println!("{}", &final_msg);
            ctx.reply_to(final_msg).await?;
        } else {
            // has not seen this message before
            let value = MessageInfo{url, count:1, link};
            db.save(&key, &value);
        };
    } else {
        get_text(&ctx).map(|text| println!("Pong, {}", text));
    }
    Ok(())
}


fn is_forward(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    ctx.update.forward_from().is_some() || ctx.update.forward_from_chat().is_some()
}

fn is_image(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    ctx.update.photo().is_some()
    // match ctx.update.photo() {
    //     Some(img) => {
    //         // dbg!(img);
    //         true
    //     },
    //     None => false
    // }
}

fn need_handle(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    // dbg!(ctx.update.chat.is_private());
    // dbg!(ctx.update.chat_id());
    // dbg!(ctx.update.id);
    // dbg!(ctx.update.forward_from());
    // dbg!(ctx.update.forward_from_chat());
    // dbg!(ctx.update.forward_from_message_id());
    // dbg!(ctx.update.forward_date());
    // dbg!(ctx.update.forward_signature());

    is_forward(&ctx)
        || ctx.update.text().to_owned()
                            .map_or(false,
                                    |ss| Url::parse(ss).is_ok())
        || is_image(&ctx)
}

async fn run(db: Arc<Mutex<MyDB>>,
             img_db: Arc<Mutex<sled::Db>>) {
    teloxide::enable_logging!();
    log::info!("Starting simple_commands_bot...");

    let bot = Bot::from_env().auto_send();

    let db = db.clone();
    teloxide::repl(bot, move |ctx| {
        let db = db.clone();
        let img_db = img_db.clone();
        async move {
            if need_handle(&ctx) {
                // TODO: think of a better way to do it.
                // Currently decided to suppress this error.
                // teloxide seem to want a RequestError, while we would want a general Error
                parse_message(&ctx, db, img_db).await.err().map(
                    |e|
                    println!("parse_message see error {:?}", e)
                );
            }
            respond(())
        }
    })
    .await;
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(MyDB::init("bot_db")));
    let img_db = Arc::new(Mutex::new(sled::open("img_db").unwrap()));
    run(db.clone(), img_db.clone()).await;
}
