use teloxide::{prelude::*, utils::command::BotCommand};
// use teloxide::types::{User, ForwardedFrom, Chat};
use teloxide::RequestError;
use std::error::Error;

use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;
use serde::{Deserialize, Serialize};

use sled;

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "display this text.")]
    Help,
    #[command(description = "handle a username.")]
    Username(String),
    #[command(description = "handle a username and an age.", parse_with = "split")]
    UsernameAndAge { username: String, age: u8 },
}

// Copied from examples, kept as reference for future adding command support
#[allow(dead_code)]
async fn answer(
    cx: UpdateWithCx<AutoSend<Bot>, Message>,
    command: Command,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match command {
        Command::Help => cx.answer(Command::descriptions()).await?,
        Command::Username(username) => {
            cx.answer(format!("Your username is @{}.", username)).await?
        }
        Command::UsernameAndAge { username, age } => {
            cx.answer(format!("Your username is @{} and age is {}.", username, age)).await?
        }
    };

    Ok(())
}

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

pub struct RocksDB {
    db: sled::Db,
}

impl KVStore for RocksDB {
    fn init(file_path: &str) -> Self {
        RocksDB { db: sled::open(file_path).unwrap()}
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
    match id_str.strip_prefix("-100") {
        Some(id) => String::from(id),
        None => id_str
    }
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
    if let Some(chat) = message.update.forward_from_chat(){
        dbg!(chat.username());
        if let (Some(username), Some(message_id))
            = (chat.username(), message.update.forward_from_message_id())
        {
            match Url::parse(&format!("https://t.me/{}/{}", username, message_id)) {
                Ok(url) => {
                    dbg!(&url);
                    Some(url)
                },
                Err(_) => None
            }
        } else {
            println!("Parse forwarded message failed");
            dbg!(chat);
            None
        }
    } else {
        None
    }
}

fn get_url(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    if let Some(ss) = ctx.update.text().to_owned() {
        match Url::parse(ss) {
            Ok(url) => Some(url),
            Err(_) => None
        }
    } else {
        None
    }
}

fn get_text(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<String> {
    if let Some(ss) = ctx.update.text().to_owned() {
        Some(String::from(ss))
    } else {
        None
    }
}

async fn parse_message(ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
                 db: Arc<Mutex<RocksDB>>) -> Result<(), RequestError> {
    let url: Option<Url>;
    let link = get_msg_link(&ctx);
    let chat_id = get_chat_id(&ctx);

    if is_forward(&ctx) {
        println!("Found a forwarded message");
        url = get_forward_msg_link(&ctx);
        if url.is_none(){
            println!("Forwarded message link parse failure.")
        }
    } else {
        println!("Found a non-forwarded message");
        url = get_url(&ctx);
        if url.is_none(){
            println!("Non-forwarded message link parse failure.")
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
            let link_msg = match &info.link {
                Some(url) => {
                    format!("第一次出现是在：{}", url)
                },
                None => {
                    // ctx.answer(format!("Last seen in private chat")).await?;
                    format!("第一次出现是在private chat")
                }
            };
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
        if let Some(text) = get_text(&ctx) {
            println!("Pong, {}", text);
        }
    }
    Ok(())
}


fn is_forward(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    ctx.update.forward_from().is_some() || ctx.update.forward_from_chat().is_some()
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

    let mut ret_val = false;
    if is_forward(&ctx) {
        ret_val = true;
    } else {
        if let Some(ss) = ctx.update.text().to_owned() {
            // dbg!(ss);
            ret_val = match Url::parse(ss) {
                Ok(_) => true,
                Err(_) => false
            }
        }
    }
    ret_val
}

async fn run(db: Arc<Mutex<RocksDB>>) {
    teloxide::enable_logging!();
    log::info!("Starting simple_commands_bot...");

    let bot = Bot::from_env().auto_send();

    let db = db.clone();
    teloxide::repl(bot, move |ctx| {
        let db = db.clone();
        async move {
            if need_handle(&ctx) {
                parse_message(&ctx, db).await?;
            }
            respond(())
        }
    })
    .await;
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(RocksDB::init("bot_db")));
    run(db.clone()).await;
}
