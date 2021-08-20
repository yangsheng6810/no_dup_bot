use teloxide::{prelude::*, utils::command::BotCommand};
use teloxide::types::{User, ForwardedFrom, Chat};
use teloxide::RequestError;
use std::error::Error;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

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

#[derive(Debug, Clone)]
pub struct MessageInfo {
    url: Url,
    count: u32,
    link: Option<Url>,
}


fn get_chat_id(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> String {
    let id = message.update.chat_id();
    let id_str = id.to_string();
    match id_str.strip_prefix("-100") {
        Some(id) => String::from(id),
        None => id_str
    }
}

fn get_msg_link(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    if message.update.chat.is_private() {
        return None;
    }
    let id = message.update.id;
    let url = match message.update.chat.username() {
            // If it's public group (i.e. not DM, not private group), we can produce
            // "normal" t.me link (accesible to everyone).
            Some(username) => format!("https://t.me/{0}/{1}/", username, id),
            // For private groups we produce "private" t.me/c links. These are only
            // accesible to the group members.
            None => format!("https://t.me/c/{0}/{1}/", get_chat_id(&message), id),
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

fn get_url(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<Url> {
    if let Some(ss) = message.update.text().to_owned() {
        match Url::parse(ss) {
            Ok(url) => Some(url),
            Err(_) => None
        }
    } else {
        None
    }
}

fn get_text(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> Option<String> {
    if let Some(ss) = message.update.text().to_owned() {
        Some(String::from(ss))
    } else {
        None
    }
}

async fn parse_message(message: &UpdateWithCx<AutoSend<Bot>, Message>,
                 db: Arc<Mutex<HashMap<Url, MessageInfo>>>) -> Result<(), RequestError> {
    let url: Option<Url>;
    let link = get_msg_link(&message);

    if is_forward(&message) {
        println!("Found a forwarded message");
        url = get_forward_msg_link(&message);
        if url.is_none(){
            println!("Forwarded message link parse failure.")
        }
    } else {
        println!("Found a non-forwarded message");
        url = get_url(&message);
        if url.is_none(){
            println!("Non-forwarded message link parse failure.")
        }
    }
    if let Some(url) = url {
        let mut db = db.lock().await;
        if let Some(info) = db.get_mut(&url){
            // has seen this message before
            info.count += 1;
            // message.answer(format!("See it {} times", info.count)).await?;
            println!("See it {} times", info.count);
            let link_msg = match &info.link {
                Some(url) => {
                    format!("第一次出现是在：{}", url)
                },
                None => {
                    // message.answer(format!("Last seen in private chat")).await?;
                    format!("第一次出现是在private chat")
                }
            };
            // message.answer(&link_msg).await?;
            let final_msg = format!("你火星了！这条消息是第{}次来到本群了，快去爬楼。{}", info.count, link_msg);
            println!("{}", &final_msg);
            message.reply_to(final_msg).await?;
        } else {
            // has not seen this message before
            db.insert(url.clone(), MessageInfo{url, count:1, link});
        };
    } else {
        if let Some(text) = get_text(&message) {
            println!("Pong, {}", text);
        }
    }
    Ok(())
}


fn is_forward(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    message.update.forward_from().is_some() || message.update.forward_from_chat().is_some()
}

fn need_handle(message: &UpdateWithCx<AutoSend<Bot>, Message>) -> bool {
    // dbg!(message.update.chat.is_private());
    // dbg!(message.update.chat_id());
    // dbg!(message.update.id);
    // dbg!(message.update.forward_from());
    // dbg!(message.update.forward_from_chat());
    // dbg!(message.update.forward_from_message_id());
    // dbg!(message.update.forward_date());
    // dbg!(message.update.forward_signature());

    let mut ret_val = false;
    if is_forward(&message) {
        ret_val = true;
    } else {
        if let Some(ss) = message.update.text().to_owned() {
            // dbg!(ss);
            ret_val = match Url::parse(ss) {
                Ok(_) => true,
                Err(_) => false
            }
        }
    }
    ret_val
}

async fn run(db: Arc<Mutex<HashMap<Url, MessageInfo>>>) {
    teloxide::enable_logging!();
    log::info!("Starting simple_commands_bot...");

    let bot = Bot::from_env().auto_send();

    let db = db.clone();
    teloxide::repl(bot, move |message| {
        let db = db.clone();
        async move {
            if need_handle(&message) {
                parse_message(&message, db).await?;
            }
            respond(())
        }
    })
    .await;
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(HashMap::<Url, MessageInfo>::new()));
    run(db.clone()).await;
}
