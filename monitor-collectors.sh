#!/bin/bash

# Create a new tmux session named 'collectors'
tmux new-session -d -s collectors

# Split the window horizontally
tmux split-window -h -t collectors

# Split both panes vertically
tmux split-window -v -t collectors:0.0
tmux split-window -v -t collectors:0.1

# In the top-left pane, show dark pool worker logs
tmux send-keys -t collectors:0.0 'sudo journalctl -u darkpool-collector-worker -f' C-m

# In the bottom-left pane, show dark pool beat logs
tmux send-keys -t collectors:0.2 'sudo journalctl -u darkpool-collector-beat -f' C-m

# In the top-right pane, show news worker logs
tmux send-keys -t collectors:0.1 'sudo journalctl -u news-collector-worker -f' C-m

# In the bottom-right pane, show news beat logs
tmux send-keys -t collectors:0.3 'sudo journalctl -u news-collector-beat -f' C-m

# Attach to the session
tmux attach-session -t collectors 