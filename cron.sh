#!/bin/sh
KIND="newiup"

start() {
	LOG_DIR=`cat /etc/newiup/newiup-conf.yaml | grep log-dir-path | tr -d "\015"`
	EXE_NUM=`ps -ef | grep -i '/etc/newiup/newiup-conf.yaml' | grep -v "grep" | wc -l`
	LOG_DIR=${LOG_DIR##*": "} 
	LOG_NAME=running_$(date +%Y%m%d).log
	LOG_PATH=$LOG_DIR/$LOG_NAME

	if [ $EXE_NUM -eq 0 ]; then
		if [ ! -d $LOG_DIR ]; then
			mkdir -p $LOG_DIR
			touch $LOG_PATH
		fi
		nohup /usr/local/bin/newiup /etc/newiup/newiup-conf.yaml >>$LOG_DIR/$LOG_NAME 2>&1 &
	fi
	echo -n $"Starting $KIND services"
	echo 
	ps aux | grep $KIND

}

stop() {
	EXE_NUM=`ps -ef | grep -i '/etc/newiup/newiup-conf.yaml' | grep -v "grep" | wc -l`
	if [ $EXE_NUM -ne 0 ]; then
		killall $KIND
	fi
	echo -n $"Shutting down $KIND services"
	echo 
}
schelog() {
	EXE_NUM=`ps -ef | grep -i '/etc/newiup/newiup-conf.yaml' | grep -v "grep" | wc -l`
	if [ $EXE_NUM -ne 0 ]; then
		killall -s USR1 $KIND
		echo -n "running Log file shows that the program is run 100 dispatching record"
		echo 
	fi
}
restart() {
	stop
	start
}



case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  restart)
        restart
        ;;
  help)
        echo $"Usage: $0 {start|stop|restart|help|schelog}"
        exit 2
        ;;
  schelog)
        schelog
        ;;
  *)
        start
        ;;
esac

exit $?