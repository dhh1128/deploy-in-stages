# deploy-in-stages
A tool for pushing data in a complex and possibly broad/deep hierarchy of machines.

Hierarchies can be partitioned for efficient use of the network, and can also be organized with time delay(s). 
The use case I wrote this tool for was one where I wanted to issue a single command on my workstation; I wanted
that command to immediately deploy data files to "head" nodes in two different datacenters, and then deploy the
same files to "head" nodes in three other datacenters an hour later. Each head node, upon receiving the data,
initiated cascading work to in turn deploy the same files, with a time delay, throughout the datacenter. I also
wanted to know what work I had completed in the overall deployment process at any given point in time.

### Installation
1. Copy this source (git pull) to ~/deploy-in-stages. You can install to other places, but the software needs to invoke copies of itself at other places in the deployment hierarchy, so your location needs to be consistent, and ~/deploy-in-stages is currently hard-coded. Changing it in your copy of the source should be easy (look for the constant at the top of the dis script).
2. (Optional) Add ~/deploy-in-stages/dis to your path (e.g., symlink to /usr/local/bin/dis).
3. Decide which machines are responsible for pushing to which other machines. Define targets.json for each machine that will be pushing. See the samples (*json*) in the root of the source.
4. Setup keyless ssh everywhere where you want an automated push. (This can be unidirectional.)
5. On each machine that is responsible for pushing, create a cron job that runs "dis more". Optionally, setup another cron job that deletes the log file from "dis more" every once in a while, so it doesn't get too big. See cron.sample in the root of the source.

### Sample Usage
* `dis help` will provide a syntax dump
* `dis queue /my/folder/ /and/my/file mytarget:~/` -- pushes a folder (recursive) and a file to ~/ on `mytarget`, where `mytarget` is defined in targets.json, or is a simple hostname
* `dis status` will check on progress
* `dis stop` will abandon work in progress
