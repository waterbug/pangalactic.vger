Deploying Dockerized crossbar and vger
======================================

Running the crossbar image with cryptosign auth over tls
--------------------------------------------------------

* set up a "home" directory for crossbar, e.g. /var/lib/crossbar, setting its
  ownership to the crossbar user (uid 242) and group (gid 242).  Any files
  added to the directory should be owned by that user and group.
* create a '.crossbar' directory under the crossbar "home" directory
* copy the file 'crossbar_config_crypto_tls.json' from the pangalactic.vger
  repository (pangalactic.vger/pangalactic/vger) into the '.crossbar'
  directory, renaming the file to 'config.json'
* copy the file 'authenticator.py' from the pangalactic.vger
  repository into the crossbar "home" directory.
* copy the file 'principals.json' from the pangalactic.vger.test repository
  (pangalactic.vger/pangalactic/vger/test) into the '.crossbar' directory under
  crossbar's "home" directory and edit it as appropriate to add the vger public
  key and an initial set of users with public keys, including at least an
  initial Global Administrator so that more users can be added at runtime (the
  "admin" user in the template file suffices but you will want to generate a
  key pair specific to your installation).
* add a certificate file and a key file for the host machine into the
  '.crossbar' directory under crossbar's "home" directory, naming them
  'server_cert.pem' and 'server_key.pem', respectively, and setting the
  permissions on 'server_key.pem' to 400.
* copy the file 'run_cb_crypto.sh.template' from the pangalactic.vger
  repository (pangalactic.vger/pangalactic/vger) to 'run_cb_crypto.sh' (or
  however you want to name it), editing as appropriate to mount the crossbar
  "home" directory to "/node" (its mount point in the crossbar docker image)
  and to map the port that you want to communicate over.
* execute the crossbar 'run' file you just created
* check the log ('docker logs [crossbar container name]') to make sure the
  crossbar container started properly.

Building and running the vger docker image
------------------------------------------

Files referenced below are found in the pangalactic.vger/docker directory.

NOTE:  it is ok to do the build inside the 'docker' directory of your
pangalactic.vger git clone -- git will ignore 'build.sh', 'Dockerfile',
'Miniconda3-latest-Linux-x86_64.sh', 'run_vger_detached.sh', and
'run_vger_interactive.sh'

* copy 'build_template.sh' to 'build.sh'
* set version of vger in 'build.sh' to the correct version
* add a copy of the latest "miniconda" install script for python 3.10
  and rename it to 'Miniconda3-latest-Linux-x86_64.sh'
* copy 'Dockerfile_template' to 'Dockerfile' and modify as necessary (e.g.
  setting the crossbar "home" directory)
* create a local "home" directory for vger, e.g. /var/lib/vger, setting its
  ownership to the crossbar user (uid 242) and group (gid 242).
  - copy the server certificate file for the host into the vger "home"
    directory, naming the file 'server_cert.pem'.
  - give group "crossbar" read access to 'server_cert.pem'.
  - copy 'config_template' into the vger "home" directory, renaming it to
    'config' and adjusting its contents as necessary.  (N.B.: "console: true"
    will redirect output to stdout and will *not* write to the logs, so it
    should only be used when the container is run interactively for testing,
    and should not be used in production.)
  - give group "crossbar" read/write access to the 'config' file.
  - add the vger private key file as 'vger.key' (this must correspond to the
    public key for the 'vger' user in crossbar's "principals" database).
  - give group "crossbar" read access to 'vger.key'.
  - create an 'extra_data' directory inside the vger "home" directory.  Its
    purpose is to hold .yaml files of any installation-specific serialized
    objects that vger should load when starting up or restarting.
  - give group "crossbar" read/execute access to 'extra_data' and read access
    to any files it contains.
* copy 'run_vger_interactive.template.sh' to 'run_vger_interactive.sh'
  (or for production, copy 'run_vger_detached.template.sh' to
  'run_vger_detached.sh') and edit so it has the correct:
  - version spec for the vger image
  - location for 'principals.db', which will be in the crossbar "home"
    directory -- i.e., make sure the crossbar "home" directory (set in the
    crossbar docker image procedure above) is bind-mounted to
    '/vger_home/crossbar' (its location in the vger image).
    NOTE:  This crossbar is configured to use the authenticator module,
    'authenticator.py', to create, write to and read from 'principals.db' and
    vger writes to it, which is why it is essential that the vger container
    runs internally as the same user [uid 242] that the crossbar container runs
    as internally.
  - N.B.:  'run_vger_detached.sh' should *only* be used for the initial start
    up of the vger container.  Thereafter, 'docker stop' and 'docker start'
    should be used to stop and start the container (use 'docker ps' to locate
    the container and get its status).  This is essential so that the vger
    database has continuity between restarts, because the database lives in an
    anonymous docker volume that is created by the container at startup and is
    "owned" by that container.

Upgrading the vger docker image and starting a new container
------------------------------------------------------------

To build and run an image for a new version of vger, the procedure is:

* stop the current vger container -- *IMPORTANT*: use the "-t 300" option to
  ensure that vger has adequate time to dump its database to a backup file
  before docker terminates the process:

    docker stop -t 300 [vger container]

* remove the current vger container

    docker rm [vger container]

* remove the current vger image

    docker rmi [vger image]

* prune the associated volume

    docker volume prune

* build the new image ("build.sh" script in the "docker" directory)

* to restore the previous database, use the database backup file
  ("db-dump-[datetime].yaml") found in the "backup" subdirectory of the vger
  home directory, which was created when vger was shut down in the first step

  - apply any transformations needed by running the "transform.py" script.
  - copy the resulting .yaml file into the "extra_data" directory in vger home
    -- vger will see it there at start up and will load it.

* remove all *.json files in vger home -- new ones will be created when vger
  starts up and deserializes the database backup.

* *IMPORTANT*:  if there are any schema changes, remove the "cache"
  subdirectory of vger home -- when vger starts up it will create a new "cache"
  with the new schema.

* start up a new container using the "run_vger_detached.sh" script.

* after vger has completed its start-up, remove the database backup file from
  the "extra_data" directory.

