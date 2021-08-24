use teloxide::{prelude::*, RequestError};
use teloxide::utils::command::BotCommand;

use std::sync::Arc;
use tokio::sync::Mutex;

use url::Url;
use serde::{Deserialize, Serialize};

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "Get help")]
    Help,
    // #[command(description = "Reply to a bot message to delete it")]
    // Delete,
    #[command(description = "Show most duplicated messages (WIP)")]
    Top,
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

// Delete the replied message
#[allow(dead_code)]
async fn delete_replied_msg(cx: &UpdateWithCx<AutoSend<Bot>, Message>,
                            with_bot_name: bool)
                            -> Result<(), RequestError> {
    match cx.update.reply_to_message() {
        Some(message) => {
            if let Some(usr) = message.from() {
                if let Some(username) = &usr.username {
                    if username.eq("no_dup_bot") {
                        println!("Start deleting message");
                        if come_from_original_author(cx) {
                            println!("Deleting message as it comes from the same author");
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
            if with_bot_name {
                cx.reply_to("Use this command in a reply to another message!").send().await?;
            }
        }
    }
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
                 db: Arc<Mutex<MyDB>>) -> Result<(), RequestError> {
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
    // dbg!(ctx.update.reply_to_message());

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

async fn handle_command(ctx: &UpdateWithCx<AutoSend<Bot>, Message>) -> Result<bool, RequestError> {
    let bot_name_str = "@no_dup_bot";
    if let Some(text) = ctx.update.text() {
        if let Ok(command) = Command::parse(text, "") {
            let with_bot_name = text.contains(bot_name_str);
            action(&ctx, command, with_bot_name).await?;
            return Ok(true)
        }
    }
    return Ok(false)
}

async fn action(
    ctx: &UpdateWithCx<AutoSend<Bot>, Message>,
    command: Command,
    with_bot_name: bool
) -> Result<(), RequestError> {
    match command {
        Command::Help => {
            if with_bot_name {
                ctx.answer(Command::descriptions()).send().await.map(|_| ())?
            }
        },
        // Command::Delete => delete_replied_msg(&ctx, with_bot_name).await?,
        Command::Top => {
            if with_bot_name {
                unimplemented!();
            }
        }
    };

    Ok(())
}


async fn run(db: Arc<Mutex<MyDB>>) {
    teloxide::enable_logging!();
    log::info!("Starting simple_commands_bot...");

    let bot = Bot::from_env().auto_send();

    // bot.set_my_commands(vec![teloxide::types::BotCommand::new("help", "delete")]).send().await.unwrap();

    let db = db.clone();
    teloxide::repl(bot, move |ctx| {
        let db = db.clone();
        async move {
            match handle_command(&ctx).await {
                Ok(true) => {
                    println!("Command handled successfully");
                },
                Ok(false) | Err(_) => {
                    if need_handle(&ctx) {
                        parse_message(&ctx, db).await?;
                    }
                },
            }
            respond(())
        }
    })
    .await;
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(MyDB::init("bot_db")));
    run(db.clone()).await;
}
