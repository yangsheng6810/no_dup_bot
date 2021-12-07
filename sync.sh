#!/usr/bin/env bash

rsync_synchronize='rsync -avzu --delete --info=progress2 -h'

case $1 in
    "down")
        echo "syncing from server"
        # $rsync_synchronize linode:git/no_dup_bot/img_db ./
        # $rsync_synchronize linode:git/no_dup_bot/bot_db ./
        # $rsync_synchronize linode:git/no_dup_bot/top_db ./
        ;;
    "up")
        echo "syncing to server"
        $rsync_synchronize ./img_db linode:git/no_dup_bot
        $rsync_synchronize ./bot_db linode:git/no_dup_bot
        $rsync_synchronize ./top_db linode:git/no_dup_bot
        scp target/x86_64-unknown-linux-musl/release/no_dup_bot linode:
        ;;
    *)
        echo "Only support ./sync.sh up|down"
        echo "up to sync to server, down to download"
        ;;
esac
