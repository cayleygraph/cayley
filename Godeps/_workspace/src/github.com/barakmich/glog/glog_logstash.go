package glog

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type logstashMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// handleLogstashMessages sends logs to logstash.
func (l *loggingT) handleLogstashMessages() {
	var conn net.Conn
	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case _ = <-l.logstashStop:
			conn.Close()
			return
		case _ = <-ticker:
			var err error
			if conn == nil {
				fmt.Fprintln(os.Stderr, "Trying to connect to logstash server...")
				conn, err = net.Dial("tcp", l.logstashURL)
				if err != nil {
					conn = nil
				} else {
					fmt.Fprintln(os.Stderr, "Connected to logstash server.")
				}
			}
		case data := <-l.logstashChan:
			lm := logstashMessage{}
			lm.Type = l.logstashType
			lm.Message = strings.TrimSpace(data)
			packet, err := json.Marshal(lm)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Failed to marshal logstashMessage.")
				continue
			}
			if conn != nil {
				_, err := fmt.Fprintln(conn, string(packet))
				if err != nil {
					fmt.Fprintln(os.Stderr, "Not connected to logstash server, attempting reconnect.")
					conn = nil
					continue
				}
			} else {
				// There is no connection, so the log line is dropped.
				// Might be nice to add a buffer here so that we can ship
				// logs after the connection is up.
			}
		}
	}
}

// StartLogstash creates the logstash channel and kicks off handleLogstashMessages.
func (l *loggingT) startLogstash() {
	l.logstashChan = make(chan string, 100)
	go l.handleLogstashMessages()
}

// StopLogstash signals handleLogstashMessages to exit.
func (l *loggingT) StopLogstash() {
	l.logstashStop <- true
}
