use teloxide::payloads::SendMessageSetters;
use teloxide::{prelude::*, net::Download, types::File as TgFile, types::PhotoSize};
use teloxide::{RequestError, ApiError};
use teloxide::utils::command::BotCommand;

use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

use url::Url;
use serde::{Deserialize, Serialize};

use std::io::{Error, ErrorKind};

use anyhow::Result;
use img_hash::ImageHash;
// use bytes::{Bytes, BytesMut, Buf, BufMut};
use bytes::BufMut;
use std::collections::BinaryHeap;
use std::collections::HashSet;

use std::env;
use once_cell::sync::OnceCell;

static BOT_NAME: &str = "no_dup_bot";
static ADMIN: OnceCell<HashSet<i64>> = OnceCell::new();
static TIME_OUT_DAYS: i64 = 10;


#[derive(BotCommand, Debug)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "Get help")]
    Help,
    #[command(description = "Reply to a bot message to delete it")]
    Delete,
    #[command(description = "Show users with most duplicated messages")]
    Top,
    #[command(description = "Show most duplicated messages")]
    Topics,
    #[command(description = "Show the number of duplicate messages I sent")]
    Me,
}

// returns true if we can get where this message is from, and it matches the
// author of the message that our bot answered
//
// Due to limitation of Telegram API, we can only go one hop for replied
// message, but no more. Therefore, we can not achieve this
#[allow(dead_code)]
fn come_from_original_author(cx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    if let Some(this_message_from) = cx.update.from() {
        dbg!(this_message_from);
        if let Some(message) = cx.update.reply_to_message() {
            dbg!(message);
            if let Some(first_message) = message.reply_to_message() {
                dbg!(first_message);
                if let Some(original_from) = first_message.from(){
                    dbg!(original_from);
                    if original_from.id == this_message_from.id {
                        return true
                    }
                }
            }
        }
    }
    // TODO: should return false, but we can not reliably detect come from
    // original author, so we temporarily always return true
    true
}

fn allows_delete(cx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    let admin_db = ADMIN.get().unwrap().clone();
    if let Some(user) = cx.update.from() {
        // dbg!(user);
        if admin_db.contains(&user.id) {
            println!("Deleting message as directed by admin {}", &user.id);
            return true
        }
    }
    println!("Admin not match!");

    // TODO: should return false, but we can not reliably detect come from
    // original author, so we temporarily always return true
    false
}

// Delete the replied message
fn reply_to_bot(cx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    if let Some(message) = cx.update.reply_to_message() {
        if let Some(usr) = message.from() {
            if let Some(username) = &usr.username {
                if username.eq(BOT_NAME) {
                    return true
                }
            }
        }
    }
    false
}

// Delete the replied message
async fn delete_replied_msg(cx: &UpdateWithCx<AutoSend<Bot>, Message>)
                            -> Result<(), RequestError> {
    match cx.update.reply_to_message() {
        Some(message) => {
            if let Some(usr) = message.from() {
                if let Some(username) = &usr.username {
                    if username.eq(BOT_NAME) {
                        println!("Start deleting message");
                        if allows_delete(cx) {
                            cx.requester
                              .delete_message(cx.update.chat_id(), message.id)
                              .await?;
                        }
                    }
                }
            } else {
                println!("Trying to delete a message without user");
            }
        }
        None => {
            // println!("Use this command in a reply to another message!");
            cx.reply_to("Please reply to a message sent by the bot!").send().await?;
        }
    }
    Ok(())
}

use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageInfo {
    #[serde(with = "url_serde")]
    url: Url,
    count: u32,
    #[serde(with = "url_serde")]
    link: Option<Url>,
    user_id: Option<i64>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct MessageKey {
    chat_id: String,
    #[serde(with = "url_serde")]
    url: Url
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct ImageKey {
    chat_id: String,
    hash_str: String
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct ImageValue {
    message: MessageKey,
    timestamp: DateTime<Utc>
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UserKey {
    chat_id: String,
    user_id: i64
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct TopUserValue {
    username: Option<String>,
    count: i64
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

impl MyDB {
    pub fn scan_prefix<P>(&self, prefix: P) -> sled::Iter
    where
        P: AsRef<[u8]>,
    {
        self.db.scan_prefix(prefix)
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

fn filter_url(ctx: &UpdateWithCx<AutoSend<Bot>, Message>, url: Option<Url>) -> Option<Url> {
    let url = url?;
    // Remove params
    // url.set_query(None);
    let mut filtered_out = false;

    let chat_id = get_chat_id(&ctx);
    if let Some(domain) = url.domain() {
        dbg!(&domain);
        match domain {
            "t.me" => {
                if let Some(mut path_segments) = url.path_segments(){
                    match path_segments.next() {
                        // filter out url link to messages in the current chat
                        Some("c") => {
                            let url_chat_id = path_segments.next();
                            // let url_message_id = path_segments.next();

                            if let Some(message_chat_id) = url_chat_id  {
                                if message_chat_id == chat_id {
                                    filtered_out = true;
                                    println!("Url {} gets filtered out with chat id {}", url, message_chat_id);
                                }
                            }
                        },
                        // filter out joinchat messages
                        Some("joinchat") => {
                            println!("Url {} gets filtered out since it is a joinchat", url);
                            filtered_out = true;
                        },
                        _ => {}
                    }
                }
            },
            "github.com" | "stackoverflow.com" => {
                // dbg!("In github.com");
                filtered_out = true;
            },
            _ => {}
        }
    }
    match filtered_out {
        true => None,
        false => Some(url)
    }
}

async fn get_hash_new(ctx: &UpdateWithCx<AutoSend<Bot>, Message>, img_to_download: &PhotoSize) -> Result<Option<String>>{
    let TgFile { file_path, .. } = ctx.requester.get_file(&img_to_download.file_id).send().await?;
    let ss = ctx.requester.download_file_stream(&file_path);
    let l = ss.collect::<Vec<_>>().await;
    // let mut count = 0;
    let mut buf = vec![];
    let mut found_error = false;
    for ii in l {
        match ii {
            Ok(b) => {
                // count += 1;
                buf.put(b);
            }
            Err(e) => {
                dbg!(e);
                found_error = true;
            }
        }
    }
    // println!("count is {:?}", count);
    if found_error {
        println!("Image download error! {:?}", &img_to_download);
        Ok(None)
    } else {
        match image::load_from_memory(&buf) {
            Ok(img) => {
                let hasher = img_hash::HasherConfig::new().to_hasher();
                let hash = Some(hasher.hash_image(&img).to_base64());
                Ok(hash)
            },
            Err(e) => {
                println!("Failed to parse image: {:?}", &e);
                Ok(None)
            }
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

async fn insert_img_hash(img_db: &Arc<Mutex<sled::Db>>, hash: &str, chat_id: &str, key: &MessageKey) -> bool {
    let img_db = img_db.lock().await;

    let img_key = ImageKey{
        chat_id: String::from(chat_id),
        hash_str: String::from(hash),
    };

    let img_value = ImageValue{
        message: key.clone(),
        timestamp: Utc::now()
    };

    let serialized_k = serde_json::to_string(&img_key).unwrap();
    let serialized_v = serde_json::to_string(&img_value).unwrap();

    if img_db.insert(serialized_k.as_bytes(), serialized_v.as_bytes()).is_err() {
        println!("database seve error when saving key {:?} with value {:?}", &hash, &key);
        false
    } else {
        true
    }
}

async fn contains_img_hash(img_db: &Arc<Mutex<sled::Db>>, hash: &str, chat_id: &str) -> bool {
    let img_db = img_db.lock().await;

    let img_key = ImageKey{
        chat_id: String::from(chat_id),
        hash_str: String::from(hash),
    };

    let serialized_k = serde_json::to_string(&img_key).unwrap();

    match img_db.contains_key(serialized_k.as_bytes()) {
        Err(_) => {
            println!("database seve error when looking for key {:?}", &img_key);
            false
        },
        Ok(ans) => ans
    }
}

// also deletes old img_db entries
async fn check_img_hash(img_db: &Arc<Mutex<sled::Db>>, hash: &str, chat_id: &str) -> Result<Option<MessageKey>> {
    // images with similarity < threshold will be considered the same
    let similarity_threshold = 4u32;
    let hash = ImageHash::from_base64(&hash).unwrap();

    // prepare an empty key so we can limit search on images from the same chat
    let empty_key = ImageKey{chat_id: String::from(chat_id), hash_str: String::from("")};
    let empty_key_str = serde_json::to_string(&empty_key).unwrap();
    // the number 20 is kind of arbitrary, but seems enough to capture the first
    // few bytes in the hash
    let prefix = &empty_key_str.as_bytes()[0..20];

    let img_db = img_db.lock().await;

    let now = Utc::now();
    let time_out_time = now.checked_sub_signed(Duration::days(TIME_OUT_DAYS)).unwrap();
    let dry_run = true;

    let mut best_hash: Option<ImageHash> = None;
    let mut best_dist: Option<u32> = None;
    let mut best_url: Option<MessageKey> = None;
    let mut count = 0;

    let mut old_img_set = HashSet::new();

    for ans in img_db.scan_prefix(prefix) {
        ans.ok().map(
            |(key, value)| {
                count += 1;
                // let key = sled_to_object::<String>(key);
                // let value = sled_to_object::<MessageKey>(value);

                let img_key = serde_json::from_str::<ImageKey>(
                    &String::from_utf8(key.to_vec()).unwrap()).unwrap();

                let img_value = serde_json::from_str::<ImageValue>(
                    &String::from_utf8(value.to_vec()).unwrap()).unwrap();

                let value = img_value.message;

                let iter_chat_id = &img_key.chat_id;
                let iter_hash = &img_key.hash_str;

                // We still need this test, as the prefix may not be perfect
                if iter_chat_id.eq(&chat_id){
                    // remove items too old
                    if img_value.timestamp < time_out_time {
                        old_img_set.insert(key.clone());
                    } else {
                        // println!("Saw {:?} {:?}", &key, &value);
                        let iter_hash = ImageHash::from_base64(&iter_hash).unwrap();
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
                    }
                }
            });
    }
    let match_ans = match best_dist {
        Some(dist) => {
            println!("The best distance is {} among all {} entries", dist, count);
            if dist < similarity_threshold {
                // the best match should update its timestamp, and removed from old img set
                if let Some(best_hash) = best_hash.clone() {
                    touch_image(&img_db, &chat_id, &best_hash, &mut old_img_set);
                }
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
    };

    // currently only try real delete on test group
    if chat_id == String::from("-413292030") {
        for key in old_img_set {
            let img_key = serde_json::from_str::<ImageKey>(
                &String::from_utf8(key.to_vec()).unwrap()).unwrap();
            if dry_run {
                println!("Dry run remove old key {:?}", &img_key)
            } else {
                match img_db.remove(&key){
                    Ok(_) => {println!("Successfully removing old key {:?} from img_db", &img_key);},
                    Err(e) => {println!("Error in removing old key {:?} from img_db: {:?}", &img_key, &e);}
                }
            }
        }
    }
    match_ans
}

fn touch_image(img_db: &MutexGuard<sled::Db>, chat_id: &str, hash_str: &ImageHash,
               old_img_set: &mut HashSet<sled::IVec>) -> bool {
    let best_key = ImageKey{
        chat_id: String::from(chat_id),
        hash_str: String::from(hash_str.to_base64())
    };
    let serialized_key = serde_json::to_string(&best_key).unwrap();

    old_img_set.remove(serialized_key.as_bytes());

    let value = img_db.get(serialized_key.as_bytes());
    if let Ok(Some(value)) = value {
        let mut v = serde_json::from_str::<ImageValue>(&String::from_utf8(value.to_vec()).unwrap()).unwrap();
        v.timestamp = Utc::now();
        let serized_v = serde_json::to_string(&v).unwrap();
        match img_db.insert(serialized_key.as_bytes(), serized_v.as_bytes()) {
            Ok(_) => {println!("Timestamp upadted for {:?}", &best_key.hash_str)},
            Err(_) => {println!("Timestamp update failed for {:?}", &best_key.hash_str)}
        };
    }
    true
}


async fn update_top_board(top_db: &Arc<Mutex<sled::Db>>, chat_id: &str, user_id: &Option<i64>, username: &Option<String>){

    if let Some(user_id) = user_id {

        let key = UserKey{
            chat_id: String::from(chat_id),
            user_id: user_id.clone(),
        };
        let key = serde_json::to_string(&key).unwrap();

        let top_db = top_db.lock().await;
        match top_db.get(key.as_bytes()) {
            Err(e) => {
                println!("top board database get error {:?} when looking for key {:?}", &e, &key);
            },
            Ok(value) => {
                let mut previous_value: i64 = 0;
                if let Some(value) = value {
                    let value = String::from_utf8(value.to_vec()).unwrap();
                    println!("In top db, finding '{:?}' returns '{}'", &key, &value);
                    let previous_user_value = serde_json::from_str::<TopUserValue>(&value).unwrap();
                    previous_value = previous_user_value.count;
                }

                let value = TopUserValue{
                    username: username.clone(),
                    count: previous_value + 1
                };
                let value = serde_json::to_string(&value).unwrap();

                if let Err(e) = top_db.insert(key.as_bytes(), value.as_bytes()) {
                    println!("database seve error {:?} when saving key {:?} with value {:?}", &e, &key, &value);
                }
            }
        }
    }
}

async fn print_topics(ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
                      db: Arc<Mutex<MyDB>>, chat_id: &str) {
    // prepare an empty key so we can limit search on images from the same chat
    let empty_key = MessageKey{
        chat_id: String::from(chat_id),
        // dummy url to make it happy
        url:Url::parse("https://example.net").unwrap()};
    let empty_key_str = serde_json::to_string(&empty_key).unwrap();
    // the number 20 is kind of arbitrary, but seems enough to capture the first
    // few bytes in the hash
    let prefix = &empty_key_str.as_bytes()[0..20];

    let db = db.lock().await;

    let mut count = 0;
    let mut heap = BinaryHeap::new();
    for ans in db.scan_prefix(prefix) {
        ans.ok().map(
            |(key, value)| {
                count += 1;
                // let key = sled_to_object::<String>(key);
                // let value = sled_to_object::<MessageKey>(value);

                let top_key = serde_json::from_str::<MessageKey>(
                    &String::from_utf8(key.to_vec()).unwrap()).unwrap();

                let value = serde_json::from_str::<MessageInfo>(
                    &String::from_utf8(value.to_vec()).unwrap()).unwrap();

                let iter_chat_id = &top_key.chat_id;
                let link = match value.link.clone() {
                    Some(url) => url.to_string(),
                    None => String::from("Link not available")
                };

                // We still need this test, as the prefix may not be perfect
                if iter_chat_id.eq(&chat_id){
                    heap.push((value.count, link));
                }
            });
    }
    let mut final_msg = String::from("火星话题排行榜：\n\n");
    let mut count = 1;
    let max_len = 20;
    let last_len_hard = 30;
    let mut last_count = 0;
    while let Some((value, link)) = heap.pop() {
        if count > max_len && (count > last_len_hard || value < last_count) {
            break;
        }
        last_count = value;
        final_msg.push_str(format!("{:<3} {:<7} {}\n",
                                   format!("{}.", &count),
                                   format!("火星{}次：", &value),
                                   &link).as_str());
        count += 1;
    }
    let chat_id = ctx.chat_id().clone();
    if let Ok(_answer_status) = ctx.requester.inner().send_message(chat_id, final_msg)
                                                     // .disable_web_page_preview(true)
                                                     .send().await {
    }
}

async fn print_top_board(ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
                         top_db: &Arc<Mutex<sled::Db>>, chat_id: &str) {
    // prepare an empty key so we can limit search on images from the same chat
    let empty_key = UserKey{chat_id: String::from(chat_id), user_id: 0};
    let empty_key_str = serde_json::to_string(&empty_key).unwrap();
    // the number 20 is kind of arbitrary, but seems enough to capture the first
    // few bytes in the hash
    let prefix = &empty_key_str.as_bytes()[0..20];

    let top_db = top_db.lock().await;

    let mut count = 0;
    let mut heap = BinaryHeap::new();
    for ans in top_db.scan_prefix(prefix) {
        ans.ok().map(
            |(key, value)| {
                count += 1;
                // let key = sled_to_object::<String>(key);
                // let value = sled_to_object::<MessageKey>(value);

                let top_key = serde_json::from_str::<UserKey>(
                    &String::from_utf8(key.to_vec()).unwrap()).unwrap();

                let value = serde_json::from_str::<TopUserValue>(
                    &String::from_utf8(value.to_vec()).unwrap()).unwrap();

                let iter_chat_id = &top_key.chat_id;
                let username = match value.username.clone() {
                    Some(user_name) => user_name,
                    None => top_key.user_id.to_string()
                };

                // We still need this test, as the prefix may not be perfect
                if iter_chat_id.eq(&chat_id){
                    heap.push((value.count, username));
                }
            });
    }
    let mut final_msg = String::from("火星排行榜：\n\n");
    let mut count = 1;
    let max_len = 20;
    let last_len_hard = 30;
    let mut last_count = 0;
    while let Some((value, username)) = heap.pop() {
        if count > max_len && (count > last_len_hard || value < last_count) {
            break;
        }
        last_count = value;
        final_msg.push_str(format!("{}. {} 火星了{}次\n", &count, &username, &value).as_str());
        count += 1;
    }
    let chat_id = ctx.chat_id().clone();
    if let Ok(_answer_status) = ctx.requester.inner().send_message(chat_id, final_msg)
                                                     .disable_web_page_preview(true)
                                                     .send().await {

        // dbg!(answer_status);
    }
    // if let Ok(_answer_status) = ctx.answer(final_msg).await {
    //     // dbg!(answer_status);
    // }
}

async fn print_my_number(ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
                         top_db: &Arc<Mutex<sled::Db>>) {
    let chat_id = get_chat_id(&ctx);
    let user_id = ctx.update.from().map_or(None, |u| Some(u.id));

    let mut final_msg = String::from("");

    if let Some(user_id) = user_id {

        let top_db = top_db.lock().await;
        let key = UserKey{
            chat_id: String::from(chat_id),
            user_id: user_id.clone(),
        };
        let key = serde_json::to_string(&key).unwrap();

        match top_db.get(key.as_bytes()) {
            Err(e) => {
                println!("top board database get error {:?} when looking for key {:?}", &e, &key);
            },
            Ok(Some(value)) => {
                    let value = String::from_utf8(value.to_vec()).unwrap();
                    println!("In top db, finding '{:?}' returns '{}'", &key, &value);
                    let value = serde_json::from_str::<TopUserValue>(&value).unwrap();
                    final_msg.push_str(format!("您已经火星{}次了！", value.count).as_str());
            },
            Ok(None) => {
                final_msg.push_str(format!("恭喜您，您还没有火星过！").as_str());
            }
        }
    } else {
        final_msg.push_str(format!("找不到您的user_id\n").as_str())
    }

    if let Ok(_answer_status) = ctx.reply_to(final_msg).await {
        // dbg!(answer_status);
    }

}

#[allow(dead_code)]
async fn cleanup_img_db(img_db: &Arc<Mutex<sled::Db>>, chat_id: &str) -> Result<()> {
    let img_db = img_db.lock().await;
    let mut count = 0;
    let now = Utc::now();
    if let Some(time_out_time) = now.checked_sub_signed(Duration::days(TIME_OUT_DAYS)) {
        for ans in img_db.iter() {
            ans.ok().map(
                |(key, value)| {
                    count += 1;
                    // let key = sled_to_object::<String>(key);
                    // let value = sled_to_object::<MessageKey>(value);

                    let img_key = serde_json::from_str::<ImageKey>(
                        &String::from_utf8(key.to_vec()).unwrap()).unwrap();

                    let img_value = serde_json::from_str::<ImageValue>(
                        &String::from_utf8(value.to_vec()).unwrap()).unwrap();

                    let iter_chat_id = &img_key.chat_id;
                    // let iter_hash = &img_key.hash_str;

                    if iter_chat_id.eq(&chat_id){
                        // remove items too old
                        if img_value.timestamp < time_out_time {
                            match img_db.remove(&key){
                                Ok(_) => {
                                    println!("Successfully removing {:?} from img_db", &img_key);
                                },
                                Err(e) => {
                                    println!("Error in removing {:?} from img_db, error {:?}", &img_key, &e);
                                }
                            }
                        }
                    }
                });
        }
    } else {
        println!("Time parse failuer when cleaning up db!");
    }
    Ok(())
}

async fn parse_message(
    ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
    db: Arc<Mutex<MyDB>>,
    img_db: Arc<Mutex<sled::Db>>,
    top_db: Arc<Mutex<sled::Db>>
) -> Result<()> {
    let mut url: Option<Url>;
    let link = get_msg_link(&ctx);
    let clean_chat_id = get_chat_id(&ctx);
    let user_id = ctx.update.from().map_or(None, |u| Some(u.id));
    let username = ctx.update.from().map_or(None,
                                            |u|
                                            match u.last_name.clone() {
                                                Some(last_name) => Some(format!("{} {}", u.first_name.clone(), last_name)),
                                                None => Some(u.first_name.clone())
                                            });
    let msg_id = ctx.update.id;

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
                match get_hash_new(&ctx, &img).await {
                    Ok(Some(hash)) => {
                        println!("Get hash {}", &hash);
                        match check_img_hash(&img_db, &hash, &clean_chat_id).await {
                            Ok(Some(key)) => {
                                println!("Found existing hash {:?} that is close", key.url);
                                url = Some(key.url.clone());
                            },
                            _ => {
                                println!("No close hash is found, use original hash {:?}", &hash);
                                url = Url::parse(&format!("https://img.telegram.com/{}", hash.clone())).ok();
                            }
                        }
                        // insert the new hash result into img_db, unless an exact key exist.
                        if let Some(url) = url.clone() {
                            if !contains_img_hash(&img_db, &hash, &clean_chat_id).await {
                                let key = MessageKey{chat_id: clean_chat_id.clone(), url:url.clone()};
                                let ans = insert_img_hash(&img_db, &hash, &clean_chat_id, &key).await;
                                if ! ans {
                                    println!("insert error, with hash {:?} and key {:?}", &hash, &key);
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
            // println!("Found a non-forward, non-image message");
            url = get_url(&ctx);
            if url.is_none(){
                println!("Non-forwarded message link parse failure.")
            }
            url = filter_url(&ctx, url);
        }
    }
    let mut my_msg_id: Option<i32> = None;
    if let Some(url) = url {
        let key = MessageKey{chat_id: clean_chat_id.clone(), url:url.clone()};
        let db = db.lock().await;
        if let Some(info) = db.find(&key){
            let mut info = info.clone();
            // has seen this message before
            info.count += 1;
            db.save(&key, &info);
            update_top_board(&top_db, &clean_chat_id, &user_id, &username).await;
            // ctx.answer(format!("See it {} times", info.count)).await?;
            println!("See it {} times", info.count);
            let link_msg = &info.link.map_or(
                format!("第一次出现是在private chat"),
                |url| format!("第一次出现是在：{}", url));
            // ctx.answer(&link_msg).await?;
            let final_msg = format!("你火星了！这条消息是第{}次来到本群了，快去爬楼。{}", info.count, link_msg);
            println!("{}", &final_msg);
            if let Ok(msg) = ctx.reply_to(final_msg).await {
                my_msg_id = Some(msg.id);
            }
        } else {
            // has not seen this message before
            let value = MessageInfo{url, count:1, link, user_id};
            db.save(&key, &value);
        };
    } else {
        get_text(&ctx).map(|text| println!("Pong, {}", text));
    }
    // delete my message if the message we replied to gets deleted
    if let Some(my_msg_id) = my_msg_id {
        let raw_chat_id = ctx.update.chat_id();
        delete_final_msg_accordingly(&ctx, raw_chat_id, msg_id, my_msg_id).await;
    }
    Ok(())
}

async fn delete_final_msg_accordingly(ctx: &UpdateWithCx<AutoSend<Bot>, Message>, chat_id: i64, msg_id: i32, my_msg_id: i32) -> bool{
    let check_wait_duration = tokio::time::Duration::from_secs(30);
    tokio::time::sleep(check_wait_duration).await;

    match ctx.requester.forward_message(chat_id, chat_id, msg_id).await {
        Ok(msg) => {
            // // message was not deleted
            // println!("Get msg {:?}", &msg);
            // println!("Message was not deleted, delete the new forward");
            if let Err(e) = ctx.requester.delete_message(chat_id, msg.id).await {
                println!("Delete failed with error {:?}", e);
            };
        },
        Err(e) => {
            // unknown error
            println!("The attempt to forward the message failed");
             match e {
                 RequestError::ApiError{kind, status_code:_}
                 if kind == ApiError::MessageToForwardNotFound ||
                     kind == ApiError::MessageIdInvalid => {
                    println!("The message was deleted, so we also delete our notification");
                },
                _ => {
                    println!("Some other error detected: {:?}", e);
                }
            }
            if let Err(e) =  ctx.requester.delete_message(chat_id, my_msg_id).await {
                println!("Clean up chat {} message {} failed with error {:?}", chat_id, my_msg_id, e);
            };
        }
    }
    true
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
    // dbg!(ctx.update.reply_to_message());

    is_forward(&ctx)
        || ctx.update.text().to_owned()
                            .map_or(false,
                                    |ss| Url::parse(ss).is_ok())
        || is_image(&ctx)
}

async fn handle_command(ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
                        db: Arc<Mutex<MyDB>>,
                        top_db: Arc<Mutex<sled::Db>>) -> Result<bool, RequestError> {
    let bot_name_str = BOT_NAME;
    if let Some(text) = ctx.update.text() {
        if let Ok(command) = Command::parse(text, bot_name_str) {
            // dbg!(&command);
            if text.contains(bot_name_str) || reply_to_bot(&ctx){
                action(&ctx, command, db, top_db).await?;
                return Ok(true)
            } else {
                return Ok(false)
            }
        }
    }
    return Ok(false)
}

async fn action(
    ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
    command: Command,
    db: Arc<Mutex<MyDB>>,
    top_db: Arc<Mutex<sled::Db>>
) -> Result<(), RequestError> {
    match command {
        Command::Help => {
            println!("Handling help request");
            ctx.answer(Command::descriptions()).send().await.map(|_| ())?
        },
        Command::Delete => {
            println!("Handling delete request");
            delete_replied_msg(&ctx).await?
        },
        Command::Top => {
            println!("Handling top board request");
            let chat_id = get_chat_id(&ctx);
            print_top_board(&ctx, &top_db, &chat_id).await;
        },
        Command::Topics => {
            println!("Show topics");
            let chat_id = get_chat_id(&ctx);
            print_topics(&ctx, db, &chat_id).await;
        },
        Command::Me => {
            println!("Handling me request");
            print_my_number(&ctx, &top_db).await;
        }
    };

    Ok(())
}

async fn run(db: Arc<Mutex<MyDB>>,
             img_db: Arc<Mutex<sled::Db>>,
             top_db: Arc<Mutex<sled::Db>>) {
    teloxide::enable_logging!();
    log::info!("Starting simple_commands_bot...");

    let bot = Bot::from_env().auto_send();

    // bot.set_my_commands(vec![teloxide::types::BotCommand::new("help", "delete")]).send().await.unwrap();

    let db = db.clone();
    teloxide::repl(bot, move |ctx| {
        let db = db.clone();
        let img_db = img_db.clone();
        let top_db = top_db.clone();
        async move {
            match handle_command(&ctx, db.clone(), top_db.clone()).await {
                Ok(true) => {
                    println!("Command handled successfully");
                },
                Ok(false) | Err(_) => {
                    if need_handle(&ctx) {
                        // TODO: think of a better way to do it.
                        // Currently decided to suppress this error.
                        // teloxide seem to want a RequestError, while we would want a general Error
                        parse_message(&ctx, db, img_db, top_db).await.err().map(
                            |e|
                            println!("parse_message see error {:?}", e)
                        );
                    }
                },
            }
            respond(())
        }
    })
    .await;
}

fn get_env() {
    let env_key = "NO_DUP_BOT_ADMIN";
    let mut admin_db = HashSet::new();
    let admin_str = match env::var_os(&env_key) {
        Some(v) => v.into_string().unwrap(),
        None => {
            println!("${} is not set", &env_key);
            String::from("0")
        }
    };
    for id in admin_str.split(":"){
        let admin = match id.parse::<i64>() {
            Ok(id) => id,
            Err(_) => 0
        };
        admin_db.insert(admin);
    }
    // only set once, so will never fail
    ADMIN.set(admin_db).unwrap();
}

#[tokio::main]
async fn main() {
    get_env();
    let db = Arc::new(Mutex::new(MyDB::init("bot_db")));
    let img_db = Arc::new(Mutex::new(sled::open("img_db").unwrap()));
    let top_db = Arc::new(Mutex::new(sled::open("top_db").unwrap()));
    run(db.clone(), img_db.clone(), top_db.clone()).await;
}
