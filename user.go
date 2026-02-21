package main

import (
	"net"
	"fmt"
)

type User struct {
	Name string
	Addr string
	C chan string
	conn net.Conn

	server *Server
}

// 用户的上线业务
func (this *User) Online(){
    this.server.mapLock.Lock()
	this.server.OnlineMap[this.Name] = this
	this.server.mapLock.Unlock()
	// 广播当前用户的上线
	this.server.Broadcast(this, "上线")
}

// 用户的下线业务
func (this *User) Offline(){
    this.server.mapLock.Lock()
	delete(this.server.OnlineMap, this.Name)
	this.server.mapLock.Unlock()
	// 广播当前用户的下线
	this.server.Broadcast(this, "下线")

}

func (this *User) SendMsg(msg string){
	this.conn.Write([]byte(msg))
}

// 用户处理消息的业务
func (this *User) DoMessage(msg string){
	if msg == "who" {
        // 查询当前都有哪些在线用户
		this.server.mapLock.Lock()
        for _, user := range this.server.OnlineMap{
			onlineMsg := fmt.Sprintf("%s 在线\n", user.Name)
			this.SendMsg(onlineMsg)
		}
		this.server.mapLock.Unlock()
	}else {
    	this.server.Broadcast(this, msg)
	}
}

// 创建一个用户
func NewUser(conn net.Conn, server *Server) *User{
	userAddr := conn.RemoteAddr().String()

	user := &User{
		Name: userAddr,
		Addr: userAddr,
		C : make(chan string),
		conn: conn,
		server: server,
	}
	go user.ListenMessage()

	return user
}

// 监听当前User channel的方法， 一旦有消息，就发送给客户端
func (this *User) ListenMessage(){
	for msg := range this.C{
		this.conn.Write([]byte(msg + "\n"))
	}

}