package main

import (
	"fmt"
	"net"
)

type Server struct{
	Ip string
	Port int
}

// 创建一个server的接口
func NewServer(ip string, port int) *Server{
	server := &Server{
		Ip: ip,
		Port: port,
	}
	return server
}

func (this *Server) Handle(conn net.Conn){
	// 当前链接的业务
	fmt.Println("Handle conn:", conn)
	fmt.Println("链接建立成功")
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