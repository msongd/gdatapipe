package main

import (
        "net"
        "log"
        "os"
        "os/signal"
        "flag"
        "net/url"
        "io"
        "sync"
)

var FROM string
var TO string
var LOG_FILE string

var fromURL *url.URL
var toURL *url.URL

func initConfig() {
  flag.StringVar(&FROM,"from","-", "Listen on this. Recognize tcp://ip:port & file://path for tcp & domain socket")
  flag.StringVar(&TO,"to","-", "Destination to forward data to. Recognize tcp://ip:port & file://path for tcp & domain socket")
  flag.StringVar(&LOG_FILE,"log","-", "Output log to a separate file")

  flag.Parse()

  if (FROM == "-") || (TO=="-") {
    flag.PrintDefaults()
    log.Fatal("Must provide correct -from & -to")
  }
  
  if (LOG_FILE != "-") {
    log.Println("Log will be output to ", LOG_FILE)
    lf, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
    if err != nil {
        log.Fatal("Failed to open log file", err)
    }
    log.SetOutput(lf)
  } else {
    log.Println("Log will be output to console")
  }
}

func validateConfig(fromStr string, toStr string) (fromURL *url.URL, toURL *url.URL) {
  f,err := url.Parse(fromStr)
  if err != nil {
    log.Fatal("Unable to parse from address")
  }
  t,err := url.Parse(toStr)
  if err != nil {
    log.Fatal("Unable to parse to address")
  }
  log.Println("From URL Scheme:" + f.Scheme)
  log.Println("From URL Host:" + f.Host)
  log.Println("From URL Path:" + f.Path)
  log.Println("To URL Scheme:" + t.Scheme)
  log.Println("To URL Host:" + t.Host)
  log.Println("To URL Path:" + t.Path)
  
  if ((f.Scheme != "tcp") && (f.Scheme != "file")) {
    log.Fatal("From URL must be tcp or file")
  }
  
  if ((t.Scheme != "tcp") && (t.Scheme != "file")) {
    log.Fatal("To URL must be tcp or file")
  }
  
  return f,t
}

func prepareSockListener(path string) net.Listener {
  _, err := os.Stat(path)
  if err == nil {
    // no such file or dir
    // socket exist
    // remove it
    err = os.Remove(path)
    if err != nil {
      log.Fatal("Unable to remove existing socket")
    }
  }
  
  l, err := net.Listen("unix", path)

  if err != nil {
        log.Fatal(err)
  }
  return l
}

func prepareNetListener(host string) net.Listener {
  l, err := net.Listen("tcp", host)

  if err != nil {
        log.Fatal(err)
  }
  return l
}

func connectUpstream(toURL *url.URL) (upC net.Conn, err error) {
  switch toURL.Scheme {
    case "tcp" : upC,err = net.Dial("tcp",toURL.Host)
    case "file": upC,err = net.Dial("file",toURL.Path)
    default:
  }
  return upC,err
}

func handleConnection(c net.Conn, toURL *url.URL) {
  var wg sync.WaitGroup
  
  defer c.Close()
  upC, err := connectUpstream(toURL)
  if err != nil {
    log.Println("Unable to connect upstream:", err)
    return
  }
  if upC == nil {
    log.Println("Bad upstream scheme")
    return
  }
  defer upC.Close()
  
  wg.Add(2)
  go  func() {
    defer wg.Done()
    io.Copy(c, upC)
  }()
  go  func() {
    defer wg.Done()
    io.Copy(upC,c)
  }()
  
  wg.Wait()
  
}


func main() {
  var l net.Listener
  
  initConfig()
  fromURL,toURL = validateConfig(FROM,TO)
  log.Println("Prepare listener")
  switch fromURL.Scheme {
    case "tcp": l=prepareNetListener(fromURL.Host)
    case "file": l=prepareSockListener(fromURL.Path)
    default: log.Fatal("From must be tcp://host:port or file://absolute_path")
  }
  defer l.Close()
  
  c := make(chan os.Signal, 1)
  signal.Notify(c, os.Interrupt)
  go func(){
    for sig := range c {
        l.Close()
        log.Fatal("Signal captured: ", sig)
      }
  }()
  
  for {
    conn,err := l.Accept()
    if err != nil {
      // error happens
      log.Println("Error accepting connection:", err)
      continue
    }
    go handleConnection(conn, toURL)
  }
}
