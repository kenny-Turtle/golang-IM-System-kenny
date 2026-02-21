package main

import (
	"fmt"
	"net"
	"sync"
)

type Server struct{
	Ip string
	Port int

	// 在线用户的列表
    OnlineMap map[string]*User
	mapLock sync.RWMutex

	// 消息广播的列表
	Message chan string 
}

// 创建一个server的接口
func NewServer(ip string, port int) *Server{
	server := &Server{
		Ip: ip,
		Port: port,
		OnlineMap: make(map[string]*User),
		Message: make(chan string),
	}
	return server
}

// 监听Message广播消息的channel的goroutine，一旦有消息，就广播给所有用户
func (this *Server) ListenMessage(){
	for msg := range this.Message{
		this.mapLock.Lock()
		for _, cli := range this.OnlineMap{
			cli.C <- msg
		}
		this.mapLock.Unlock()
	}
}

// 广播消息的方法
func (this *Server) Broadcast(user *User, msg string){
	sendMsg := "[" + user.Addr + "]" + user.Name + " : " + msg

	this.Message <- sendMsg
}

func (this *Server) Handle(conn net.Conn){
	// 当前链接的业务
	fmt.Println("Handle conn:", conn)
	fmt.Println("链接建立成功")

	user := NewUser(conn)

	// 当前用户上线了，将用户加入到onlinemap中
    this.mapLock.Lock()
    this.OnlineMap[user.Name] = user
	this.mapLock.Unlock()
	// 广播当前用户的上线
	this.Broadcast(user, "has joined the chat room")
}

// 启动服务器的接口
func (this *Server) Start(){
	// socket listen   listen套接字
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil{
		fmt.Println("Listen failed, err:", err)
		return
	}
	// close listen socket
	defer listen.Close()

	// 启动监听Message广播消息的channel的goroutine
	go this.ListenMessage()

	for {
		// accept
    	conn, err := listen.Accept()
		if err != nil{
			fmt.Println("Accept failed, err:", err)
			continue
	}
	// do handle
	go this.Handle(conn)

	}


}