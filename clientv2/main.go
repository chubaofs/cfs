// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

//
// Usage: ./client -c fuse.json &
//
// Default mountpoint is specified in fuse.json, which is "/mnt".
//

import (
	"context"
	"flag"
	"fmt"
	syslog "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	sysutil "github.com/chubaofs/chubaofs/util/sys"

	"github.com/jacobsa/daemonize"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	cfs "github.com/chubaofs/chubaofs/clientv2/fs"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	MaxReadAhead = 512 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"
	LoggerOutput = "output.log"

	ModuleName            = "fuseclient"
	ConfigKeyExporterPort = "exporterKey"

	ControlCommandSetRate = "/rate/set"
	ControlCommandGetRate = "/rate/get"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	configFile       = flag.String("c", "", "FUSE client config file")
	configVersion    = flag.Bool("v", false, "show version")
	configForeground = flag.Bool("f", false, "run foreground")
)

var GlobalMountOptions []proto.MountOption

func init() {
	GlobalMountOptions = proto.NewMountOptions()
	proto.InitMountOptions(GlobalMountOptions)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *configVersion {
		fmt.Printf("ChubaoFS Client v2\n")
		fmt.Printf("Branch: %s\n", BranchName)
		fmt.Printf("Commit: %s\n", CommitID)
		fmt.Printf("Build: %s %s %s %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
		os.Exit(0)
	}

	if !*configForeground {
		if err := startDaemon(); err != nil {
			fmt.Printf("Mount failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	/*
	 * We are in daemon from here.
	 * Must notify the parent process through SignalOutcome anyway.
	 */

	cfg, _ := config.LoadConfigFile(*configFile)
	opt, err := parseMountOption(cfg)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	exporter.Init(ModuleName, opt.Config)

	level := parseLogLevel(opt.Loglvl)
	_, err = log.InitLog(opt.Logpath, LoggerPrefix, level, nil)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	outputFilePath := path.Join(opt.Logpath, LoggerPrefix, LoggerOutput)
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)

	syslog.Println("*** Final Mount Options ***")
	for _, o := range GlobalMountOptions {
		syslog.Println(o)
	}
	syslog.Println("*** End ***")

	if err = sysutil.RedirectFD(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}

	registerInterceptedSignal(opt.MountPoint)

	mfs, err := mount(opt)
	if err != nil {
		log.LogFlush()
		daemonize.SignalOutcome(err)
		os.Exit(1)
	} else {
		daemonize.SignalOutcome(nil)
	}

	if err = mfs.Join(context.Background()); err != nil {
		log.LogFlush()
		syslog.Printf("mfs Joint returns error: %v", err)
		os.Exit(1)
	}
}

func startDaemon() error {
	cmdPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path, err(%v)", err)
	}

	configPath, err := filepath.Abs(*configFile)
	if err != nil {
		return fmt.Errorf("startDaemon failed: cannot get absolute command path of config file(%v) , err(%v)", *configFile, err)
	}

	args := []string{"-f"}
	args = append(args, "-c")
	args = append(args, configPath)

	env := []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
	}

	err = daemonize.Run(cmdPath, args, env, os.Stdout)
	if err != nil {
		return fmt.Errorf("startDaemon failed: daemon start failed, cmd(%v) args(%v) env(%v) err(%v)\n", cmdPath, args, env, err)
	}

	return nil
}

func mount(opt *proto.MountOptions) (*fuse.MountedFileSystem, error) {
	super, err := cfs.NewSuper(opt)
	if err != nil {
		log.LogError(errors.Stack(err))
		return nil, err
	}

	http.HandleFunc(ControlCommandSetRate, super.SetRate)
	http.HandleFunc(ControlCommandGetRate, super.GetRate)
	http.HandleFunc(log.SetLogLevelPath, log.SetLogLevel)
	go func() {
		fmt.Println(http.ListenAndServe(":"+opt.Profport, nil))
	}()

	exporter.RegistConsul(super.ClusterName(), ModuleName, opt.Config)

	opt.Rdonly = super.TokenType() == int8(proto.ReadOnlyToken)

	server := fuseutil.NewFileSystemServer(super)
	mntcfg := &fuse.MountConfig{
		FSName:                  "chubaofs-" + opt.Volname,
		Subtype:                 "chubaofs",
		ReadOnly:                opt.Rdonly,
		DisableWritebackCaching: true,
	}

	if opt.WriteCache {
		mntcfg.DisableWritebackCaching = false
	}

	// define extra options
	mntcfg.Options = make(map[string]string)
	mntcfg.Options["allow_other"] = ""

	mfs, err := fuse.Mount(opt.MountPoint, server, mntcfg)
	if err != nil {
		return nil, err
	}

	return mfs, nil
}

func registerInterceptedSignal(mnt string) {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigC
		syslog.Printf("Killed due to a received signal (%v)\n", sig)
	}()
}

func parseMountOption(cfg *config.Config) (*proto.MountOptions, error) {
	var err error
	opt := new(proto.MountOptions)
	opt.Config = cfg

	proto.ParseMountOptions(GlobalMountOptions, cfg)

	rawmnt := GlobalMountOptions[proto.MountPoint].GetString()
	opt.MountPoint, err = filepath.Abs(rawmnt)
	if err != nil {
		return nil, errors.Trace(err, "invalide mount point (%v) ", rawmnt)
	}

	opt.Volname = GlobalMountOptions[proto.VolName].GetString()
	opt.Owner = GlobalMountOptions[proto.Owner].GetString()
	opt.Master = GlobalMountOptions[proto.Master].GetString()
	opt.Logpath = GlobalMountOptions[proto.LogDir].GetString()
	opt.Loglvl = GlobalMountOptions[proto.LogLevel].GetString()
	opt.Profport = GlobalMountOptions[proto.ProfPort].GetString()
	opt.IcacheTimeout = GlobalMountOptions[proto.IcacheTimeout].GetInt64()
	opt.LookupValid = GlobalMountOptions[proto.LookupValid].GetInt64()
	opt.AttrValid = GlobalMountOptions[proto.AttrValid].GetInt64()
	opt.ReadRate = GlobalMountOptions[proto.ReadRate].GetInt64()
	opt.WriteRate = GlobalMountOptions[proto.WriteRate].GetInt64()
	opt.EnSyncWrite = GlobalMountOptions[proto.EnSyncWrite].GetInt64()
	opt.AutoInvalData = GlobalMountOptions[proto.AutoInvalData].GetInt64()
	opt.UmpDatadir = GlobalMountOptions[proto.WarnLogDir].GetString()
	opt.Rdonly = GlobalMountOptions[proto.Rdonly].GetBool()
	opt.WriteCache = GlobalMountOptions[proto.WriteCache].GetBool()
	opt.KeepCache = GlobalMountOptions[proto.KeepCache].GetBool()
	opt.FollowerRead = GlobalMountOptions[proto.FollowerRead].GetBool()
	opt.Authenticate = GlobalMountOptions[proto.Authenticate].GetBool()
	if opt.Authenticate {
		opt.TicketMess.ClientKey = GlobalMountOptions[proto.ClientKey].GetString()
		opt.TicketMess.TicketHost = GlobalMountOptions[proto.TicketHost].GetString()
		opt.TicketMess.EnableHTTPS = GlobalMountOptions[proto.EnableHTTPS].GetBool()
		if opt.TicketMess.EnableHTTPS {
			opt.TicketMess.CertFile = GlobalMountOptions[proto.CertFile].GetString()
		}
	}
	opt.TokenKey = GlobalMountOptions[proto.TokenKey].GetString()

	if opt.MountPoint == "" || opt.Volname == "" || opt.Owner == "" || opt.Master == "" {
		return nil, errors.New(fmt.Sprintf("invalid config file: lack of mandatory fields, mountPoint(%v), volName(%v), owner(%v), masterAddr(%v)", opt.MountPoint, opt.Volname, opt.Owner, opt.Master))
	}

	return opt, nil
}

// ParseLogLevel returns the log level based on the given string.
func parseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	return level
}
