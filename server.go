package main

import (
	"fmt"
	"net"
	"sync"
	"io"
	"time"
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

	user := NewUser(conn, this)

	user.Online()
	/* 将上线功能分装到user类里
	// 当前用户上线了，将用户加入到onlinemap中
    this.mapLock.Lock()
    this.OnlineMap[user.Name] = user
	this.mapLock.Unlock()
	// 广播当前用户的上线
	this.Broadcast(user, "上线")
    */

	// 监听用户是否活跃的channel
	isLive := make(chan bool)

	// 接收客户端发送的消息 
	go func(){
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n == 0{
				user.Offline()
				return
			}

			if err != nil && err != io.EOF {
				fmt.Println("Conn Read err:", err)
				continue
			}

			// 提取用户的消息（去除‘\n'）
			msg := string(buf[:n-1])

			// 将得到的消息进行广播
			user.DoMessage(msg)

			// 用户的任意消息，代表当前用户是一个活跃的
			isLive <- true
		}
	}()

	// 当前handler阻塞
	for {
	    select {
		case <- isLive:
			// 说明当前用户是活跃的，应重置定时器
			// 不做任何事情，为了激活select，更新下面的定时器

		case <- time.After(time.Second * 10):
			// 已经超时
			// 将当前的User强制地关闭下线

			user.SendMsg("您已超时下线")

			// 销毁用的资源
			close(user.C)

			// 关闭链接
			conn.Close()

			// 退出当前的Handler
			return //runtime.Goexit()
		}

	}
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