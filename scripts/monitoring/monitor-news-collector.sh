#!/bin/bash

# Create a new tmux session named 'news-collector'
tmux new-session -d -s news-collector

# Split the window horizontally
tmux split-window -h -t news-collector

# In the left pane, show worker logs
tmux send-keys -t news-collector:0.0 'sudo journalctl -u news-collector-worker -f' C-m

# In the right pane, show beat logs
tmux send-keys -t news-collector:0.1 'sudo journalctl -u news-collector-beat -f' C-m

# Attach to the session
tmux attach-session -t news-collector 