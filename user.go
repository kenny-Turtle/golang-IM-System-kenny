package main

import (
	"net"
	"fmt"
	"strings"
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
	} else if len(msg) > 7 && msg[:7] == "rename|" {
        // 消息格式： rename|张三
		newName := strings.Split(msg, "|")[1]
		// 检查新的用户名是否存在
		_, ok := this.server.OnlineMap[newName]
		if ok{
			this.SendMsg("该用户名已存在\n")
		}else {
			this.server.mapLock.Lock()
            delete(this.server.OnlineMap, this.Name)
			this.server.OnlineMap[newName] = this
			this.server.mapLock.Unlock()

			this.Name = newName
			this.SendMsg("您已成功重命名为：" + newName + "\n")
		}
	} else {
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
		_, err := this.conn.Write([]byte(msg + "\n"))
		if err != nil{
			fmt.Println("Send msg err:", err)
		}
	}

}