# VGER Admin Guide

The VGER (Virtual Galactic Engineering Repository) service consists of 2 docker
components:

1. A message service, **crossbar** (container name: **crossbar_crypto**)
2. A repository service module, **vger** (container name: **vger_x.x.x**)

## Getting the status of the docker containers

    docker ps -a

On the server, the output will look like:

`CONTAINER ID   IMAGE                 COMMAND                  CREATED         STATUS        PORTS                                                 NAMES`  
`cfd9960b0ab3   vger:x.x.x            "bash -c 'exec /usr/…"   2 weeks ago     Up 25 hours                                                         vger_x.x.x`  
`6397ca2ca59b   crossbario/crossbar   "crossbar start --cb…"   2 months ago    Up 1 week     8000/tcp, 0.0.0.0:8080->8080/tcp, :::8080->8080/tcp   crossbar_crypto`  

## Starting and stopping the docker containers

1. **Starting the crossbar_crypto container**

The **crossbar_crypto** container must be up and running before the repository
service container (**vger_x.x.x**) can start, since **vger** automatically
connects to **crossbar** at startup and will fail to start if **crossbar** is
not available.

If the **crossbar_crypto** container exists but is stopped (which can be
determined using the *docker ps -a* command above), simply use the following
command:

    docker start crossbar_crypto

If the **crossbar_crypto** container does not exist (i.e., it does not appear
in the output of the `docker ps -a` command), the following procedure will
create it and start its container:

    cd /var/lib/crossbar
    ./run_cb_crypto.sh

2.  **Starting the vger_x.x.x container**

If the **vger_x.x.x** container exists and is stopped:

    docker start vger_x.x.x

If the **vger_x.x.x** container does not exist, the following procedure will create
and start the container:

    ./run_vger_detached.sh

(This script can be created from the file `run_vger_detached.template.sh` in
the docker folder.)

3. **Stopping the vger_x.x.x docker container**

In some situations, the **vger_x.x.x** container can lose its connection to
**crossbar** and be unreachable -- although the **vger_x.x.x** container may
still be running, the client will display an error message when attempting to
login to the repository. In those situations, the **vger_x.x.x** container must
be stopped and then started again.

To stop the **vger_x.x.x** container properly, it is necessary to use the the
*-t* flag with the docker *stop* command in order to give **vger** a suitable
grace period in which to complete the database backup that it does at shutdown
-- this is because the default grace period allowed by the docker *stop*
command before it issues a *sigkill* to the process is 10 seconds, which is not
sufficient for **vger** to complete its database backup, which typically takes
around 1 minute for a large database.  So it is recommended to allow 100
seconds -- the process will exit as soon as the database backup completes
anyway, even if it takes less than 100 seconds.  Hence a suitable command is:

    docker stop -t 100 vger_x.x.x

Once the **vger_x.x.x** container has stopped, it can immediately be restarted
using the docker *start* command above.

## Monitoring the vger server logs

To ensure that the **vger_x.x.x** container has started properly, its logs can
be monitored using the following procedure:

    cd [vger home directory]/log
    tail -f orb_log

That form of the *tail* command will display the realtime logging output of
the **vger** server, which is being written to the *orb_log* file.  Since
**vger** does log rotation, the command will eventually lose its connection to
the output of the process when the log file is rotated, so if you see the
output pause for a long time and want to continue monitoring, simply terminate
the *tail* process and re-run the *tail* command, which will display output to
the new *orb_log* file.

When the **vger** process has successfully started, can typically take
a few minutes with a large database, the output shown by the above *tail*
command should look like the following:

`2024-12-07 14:50 * performing self-audit of deletions ...`
`2024-12-07 14:50   passed.`
`2024-12-07 14:50   validating all HW and Template ids ...`
`2024-12-07 14:50   all HW and Template ids are correct.`
`2024-12-07 14:50 procedures registered`  

The final output line of the startup sequence is *procedures registered*, which
indicates that all of the **vger** *rpcs* (remote procedure calls) have
successfully been registered with the **crossbar** message server.

