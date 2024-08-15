package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

type ConnectionPool struct {
	conns  chan *sql.DB
	mu     sync.Mutex
	closed bool
}

const DBURL = "some url"

func NewConnectionPool(maxConnections int) (*ConnectionPool, error) {
	p := &ConnectionPool{
		conns: make(chan *sql.DB, maxConnections),
	}

	for i := 0; i < maxConnections; i++ {
		db, err := sql.Open("mysql", DBURL)
		if err != nil {
			// close any connection already opened.
			p.Close()
			return nil, err
		}

		db.SetMaxOpenConns(1)
		db.SetConnMaxLifetime(1 * time.Hour)

		p.conns <- db
	}

	return p, nil
}

// implement a ping mechanism to verify if connetion is really active. db.ping()
func (c *ConnectionPool) GetConnection(ctx context.Context) (*sql.DB, error) {

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	c.mu.Unlock()

	select {
	case conn := <-c.conns:
		return conn, nil
	// to manage timeouts or cancellations when getting a connection.
	// This will provide more control in high-concurrency situations.
	case <-ctx.Done():
		return nil, ctx.Err()
	// handle cases where the pool is empty and all connections are in use.
	// You might want to block, return an error, or create a new connection temporarily.
	default:
		return nil, fmt.Errorf("no active connections")
	}
}

func (c *ConnectionPool) Close() {

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true // Set the closed flag to true
	close(c.conns)
	c.mu.Unlock()

	for len(c.conns) > 0 {
		// Closing the conns channel while it's still being read from could cause a panic. It's safer to handle this with a more graceful shutdown.
		// Drain the channel before closing it, ensuring all connections are properly closed without sending more data.
		conn := <-c.conns
		if err := conn.Close(); err != nil {
			log.Printf("error closing connection:  %v", err)
		}
	}
}
