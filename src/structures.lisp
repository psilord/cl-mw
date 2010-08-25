;; Copyright 2010 Peter K. Keller (psilord@cs.wisc.edu)
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you
;; may not use this file except in compliance with the License. You may
;; obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied. See the License for the specific language governing
;; permissions and limitations under the License.

(in-package #:cl-mw)

;; Simple identification functions, so I can give names to everything.
;; I can't use interned keywords for this since there might be so many tasks
;; that I'll run out of space storing them all! Also, the identification
;; value must be able to exist in different process spaces.
(let ((count 0))
  (defun gen-mw-result-id ()
    (let ((id (format nil "RESULT-~A" count)))
      (incf count)
      id)))

(let ((count 0))
  (defun gen-mw-slave-id ()
    (let ((id (format nil "SLAVE-~A" count)))
      (incf count)
      id)))

(let ((count 0))
  (defun gen-mw-task-id ()
    (let ((id (format nil "TASK-~A" count)))
      (incf count)
      id)))

;; The mw state structure which represents a task and where it should go.
(defstruct mw-task
  ;; a task id
  (id (gen-mw-task-id))

  ;; What package is this algorithm function in?
  (pkge nil)

  ;; the appropriate algorithm this task requires
  (algorithm nil)

  ;; If this task is destined to a specific slave id, this is it.
  ;; Defining this means the task is a ordered task.
  (sid nil)

  ;; the producer can associate a form with a tag which is transmitted
  ;; unmolested to the consumer in the result structure
  (tag nil)

  ;; At what time in epoch seconds was this task queued for processing
  ;; This is set just before it is queued, so don't set it here just in case
  ;; we make it, but don't queue it for a long time.
  (queue-time nil)

  ;; At what time was this task sent over to a slave (or collection of
  ;; slaves) to be processed?
  (sent-time nil)

  ;; If the task has to be reclaimed due to a failed speculation or another
  ;; failure, like it is around long than its ttl, should we try again?
  (retry t)

  ;; this states that if the task was for a specific sid, but the sid was
  ;; dead by the time I get around to scheduling this task, just give it
  ;; to any old slave.
  (do-it-anyway nil)

  ;; The actual form created by the producer to be worked on by the slave
  (packet '()))

;; This structure defines a connected slave ready for work.
;; Any slave can run any one of the slave algos
(defstruct mw-slave
  ;; The slave-id
  (sid (gen-mw-slave-id))

  ;; status is :unstabled, :connecting, :idle, :busy, :disconnected
  ;; depending if it is running a task or not among other things.
  ;; Unstabled means the slave is not in the slave stable.
  (status :unstabled)

  ;; The time stamp when we entered whatever current state we are in.
  (last-heard-from (get-universal-time))

  ;; who is the domain name or ip address from where the slave connected
  (who nil)
  ;; port is the port of connection from the slave
  (port 0)

  ;; connected-time is when the slave connected to the master
  (connected-time 0)

  ;; What is the maximum number of tasks that I can package up in one
  ;; network packet to be processed by this specific slave. The queue
  ;; will always have this number of entries in it or less.
  (task-group 1)

  ;; What is the maximum number of results up to which the slave may hold
  ;; before sending back the packet of results?
  (result-group 1)

  ;; A slave could be provisioned to run ordered tasks (which know
  ;; some internal state about the slave and utilize it, like a cache
  ;; for example).
  ;;
  ;; Valid options are: :unordered, :intermingle, :ordered
  ;;
  ;; :unordered - Run only unordered tasks.
  ;; :intermingle - Slave allows ordered use, but give random tasks for eff.
  ;; :ordered - Slave will ONLY execute specified seq tasks or be idle.
  (ordered :unordered)

  ;; task queue is a queue of task ids  which have been
  ;; given to the slave as one big chunk to be processed, in order, in
  ;; the queue. As we get results back from this slave we pop the
  ;; matching task ids to the results off of the queue. When this gets to
  ;; be empty, the slave goes back to idle.
  (pending-task-queue (make-queue))
  ;; How many tasks are pending to be completed by the slave
  (num-pending-tasks 0)

  ;; The total of the next two attributes states how many tasks were run
  ;; through the slave all together.

  ;; How many tasks did this slave successfuly complete and it was the first
  ;; result for the task?
  (num-good-speculations 0)

  ;; How many tasks did this slave complete, but it turns out that some other
  ;; slave had done it first and our result got ignored?
  (num-bad-speculations 0)

  ;; When I want to send data to the slave, this is the controller function
  ;; of the associated packet buffer which knows how to do that.
  (controller nil)
  )

;; The result from a slave computation
;; stype is which slave type it came from
;; sid is the specific slave id from which this result came
;; tag is an arbitrary form the producer supplied which the consumer may use,
;;  it is transferred without alterations from the producer to the consumer
;;  through the slave.
;; compute-time is how long it took the slave to compute the information.
;; packet is the resultant form from the slave given back to the consumer
(defstruct mw-result
  ;; The unique id value of this result
  (id (gen-mw-result-id))
  ;; The task id which produced this result
  (tid nil)
  ;; The algorithm which produced this result
  (algorithm nil)
  ;; The slave-id which produced this result
  (sid nil)
  ;; The tag from the producer being given back to the consumer
  (tag nil)
  ;; How long it took to compute this result (master figures out when
  ;; it matches the result to the initiating task)
  (compute-time 0)
  ;; The actual result from the slave
  (packet '()))

(defstruct mw-stable                    ; Slave TABLE. That's funny!
  ;; Any slave we currently know and care about is recorded here.
  ;; Key is "who:port", Value is slave structure
  (slaves (make-hash-table :test #'equal))

  ;; These are views into the slave table by various lookup keys.
  ;; A slave may only be in one A-slaves AND A-slaves-by-sid set at a time.
  ;;
  ;; These are keyed by "who:port" or sid, value is "who:port" in slaves table
  (connecting-slaves (make-hash-table :test #'equal))
  (connecting-slaves-by-sid (make-hash-table :test #'equal))
  (idle-slaves (make-hash-table :test #'equal))
  (idle-slaves-by-sid (make-hash-table :test #'equal))
  (busy-slaves (make-hash-table :test #'equal))
  (busy-slaves-by-sid (make-hash-table :test #'equal))
  (shutting-down-slaves (make-hash-table :test #'equal))
  (shutting-down-slaves-by-sid (make-hash-table :test #'equal))
  (disconnected-slaves (make-hash-table :test #'equal))
  (disconnected-slaves-by-sid (make-hash-table :test #'equal))

  ;; Here we keep a view of the kind of slave it is
  ;;
  ;; Key is :intermingle, :ordered, or :unordered, Value is a hash
  ;; table with key "who:port" and value "who:port" in slaves table.
  (kind (mihtequal
         :ordered (mihtequal)
         :intermingle (mihtequal)
         :unordered (mihtequal)))

  ;; Key is :intermingle, :ordered, or :unordered, Value is a hash
  ;; table with key "sid" and value "who:port" in slaves table.
  (kind-by-sid (mihtequal
                :ordered (mihtequal)
                :intermingle (mihtequal)
                :unordered  (mihtequal)))

  )

;; Record various configuration values for various parts of the system.
;; All defaults are set in mw-parse-argv
(defstruct mw-conftable

  ;; What style is this instance, a :master or a :slave?
  (style nil)

  ;; This is the master host to which the clients will connect.
  (master-host nil)

  ;; This is the master's port to which the clients will connect.
  (master-port nil)

  ;; If this isn't set, use the packet buffer closure defaults
  (max-read-buffer-size nil)

  ;; Future expansion in the codes....
  (max-write-buffer-size nil)

  ;; When waiting for a client to talk to us when we're expecting a response,
  ;; how long to we wait?
  (client-timeout nil)

  ;; A file that is where I will write the audit log. If nil, use
  ;; *standard-output*
  (audit-file nil)

  ;; A file which represents the health of the master and how many resources
  ;; it desires and has acquired. If not specified, no file is written.
  (resource-file nil)

  ;; How often, in seconds, should the resource file be updated?
  (resource-file-update-interval nil)

  ;; How many tasks should be packed into a packet when sent to a slave?
  (slave-task-group nil)

  ;; How many results should be grouped into a packet when sent to the master?
  (slave-result-group nil)

  ;; What slave executable should be specified when writing the
  ;; hearbeat file.  If no command line arguments set this up, it'll
  ;; get set to the fully qualified path of the master executable,
  ;; since that is precisely the same executable as the slave. This
  ;; ends up in the resource file and makes it much easier for a batch
  ;; system to deal with it.
  (slave-executable nil)

  ;; This data is written out to the resource file in addition to
  ;; being sent by any client to the master upon identification. This
  ;; is a weak membership test simply used by the master and clients
  ;; by a higher level batch system for bookeeping purposes and to
  ;; make it harder for many masters and clients to get confused with
  ;; each other in the samea dministrative domain.
  (member-id nil)

  ;; What is the full path to the intresting shared librarys this executable
  ;; is using?
  (shared-libs *library-dependencies*)

  ;; This is a special variable used when the slave has just read in a
  ;; resource file. If the resource file states the computation is
  ;; finished, we mark that here, and the slave will exit very quickly
  ;; thereafter.  We have to put it here instead of in the worker
  ;; structure due to when this structure gets bound in relation to
  ;; parsing argv, and when the worker structure gets bound.
  (computation-finished nil)

  )

;; This contains the total internal state of the master process
(defstruct mw-master
  ;; when a master, this is the master's command ip/port
  (master-ip nil)
  (master-port 0)

  ;; When the results start pouring in, this keeps track of them until they
  ;; end up shoved to the consumer. These are result structures!
  (results-queue (make-queue))
  ;; How many results are in the queue
  (num-results 0)

  ;; When a ordered/intermingle slave connects, it gets put into
  ;; this list for the master algorithm to use.
  (connected-ordered-slaves nil)
  ;; When a ordered/intermingle disconnects, it goes here so the
  ;; master algo can do something menaingful with it.
  (disconnected-ordered-slaves nil)

  ;; How many slaves in each bucket do we need, and we fill the
  ;; buckets in the order of :ordered, then :intermingle, then
  ;; :unordered as we gather slaves to us. If we happen to get more
  ;; slaves than we need in any particular bucket, they end up in the
  ;; unordered acquired bucket.
  (slaves-desired (mihtequal
                   :ordered 0
                   :intermingle 0
                   :unordered 0))

  (slaves-acquired (mihtequal
                    :ordered 0
                    :intermingle 0
                    :unordered 0))

  ;; Either this will be set to a thunk which will update the resource file,
  ;; or it'll be this default which does nothing.
  (resource-file-update-function #'(lambda () t))

  ;; When writing out the resource file we mention if we know the computation
  ;; is :in-progress, or :finished. This tells the batch system above us
  ;; that it should try to honor the file, or start to tear everything down.
  ;; In the case of a slave reading a resource file where the computation
  ;; status is :finished, it will immediately exit with a zero exit status
  ;; since in effect the master told it to shut down.
  (computation-status nil)

  )

(defstruct mw-worker
  ;; The controller back to the single master which controls us.
  (controller nil)

  ;; Did the master disconnect at the network level?
  (master-disconnect nil)

  ;; Did the master send an explicit shutdown to the slave?
  (explicit-shutdown nil)

  ;; The queue of tasks the worker is supposed to compute
  (task-queue (make-queue))

  ;; How many results should be queued before sending them back in one network
  ;; transaction.
  (result-queue (make-queue))
  ;; Current number of available results to send back
  (num-results 0)
  (result-grouping 1)

  (total-results-completed 0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; Which slaves are working on a task's behalf, and if a task has come back
;; from one of the slaves, then we store the task-id into the computed tasks
;; hash so if we get another result for the same task from a different slave
;; later, we know that we can ignore it.
(defstruct mw-speculation
  ;; have we seen the result from any slave yet for this task?
  (computed-result nil)

  ;; A slave computing this task is recorded into this set.
  ;;
  ;; Key is slave-id, Value is t (XXX maybe value should be sent-time
  ;; and resolve-speculation should return values of (first and
  ;; sent-time) so the math can bed one correctly for how long it took
  ;; a task to be computed?
  (slaves (make-hash-table :test #'equal)))

;; A target-number is a notion of how many tasks of a certain kind, or
;; any kind, are present and pending to run in the taskjar. This
;; allows the master-algo to easily pump more tasks into the system.
(defstruct mw-target-number
  ;; What are the user set target-numbers for the algorithms, if any.
  ;; Keyed by algorithm name, value is target-number level (integer)
  (algorithm-target-numbers (mihtequal))
  ;; How many of each algorith are pending to run?  Keyed by algorithm
  ;; name, value is integer count of pending tasks.
  (algorithm-pending-tasks (mihtequal))
  ;; What is the target-number of all tasks?
  (general-target-number 0)
  ;; This is different that what mw-num-runnable-tasks provides in
  ;; that we KNOW that pending tasks are strictly not running.
  (general-pending 0)
  )

(defstruct mw-taskjar

  ;; This stores the real task structures until a result has been computed
  ;; for them. We have this store because we can speculate the same task on
  ;; multiple slaves and have to be able to talk about the task when a slave
  ;; comes back with a result for it.
  ;;
  ;; Keyed by task-id, Value is actual task structure.
  (tasks (make-hash-table :test #'equal))

  ;; This stores the task id for tasks which can be executed by any
  ;; slave.  When task-ids are taken from this set and given to the
  ;; slave, the slave assumes ownership of the task-id. Sequential
  ;; task ids are not in this set.
  ;;
  ;; Keyed by task id, Value is task id
  (unordered-tasks (make-hash-table :test #'equal))

  ;; This stores the task id for ordered tasks destined for a
  ;; specific unique slave.  When task-ids are taken from this set
  ;; and given to the slave, the slave assumes ownership of the
  ;; task-id. Unordered tasks are not in this set. There is only ever
  ;; ONE speculation for a ordered task, and that is on the slave to
  ;; which it is destined.
  ;;
  ;; Keyed by slave id, value is a queue of task ids to be run in order.
  (ordered-tasks (make-hash-table :test #'equal))

  ;; When we assign a task to one or more slaves, we are speculating
  ;; that the task will be completed by one of them. Here we record
  ;; which slave(s) is/are processing which task and if we've gotten
  ;; an answer back already for the task. When an answer does come
  ;; back, we remove the slave that sent it from the speculation
  ;; structure and mark that we have a result. Of course, any result we get
  ;; from any slave after the first one is ignored. When all slaves are
  ;; removed, we can remove the speculation entry for the task.
  ;;
  ;; NOTE! Sequential tasks by definition are only speculated ONCE. They
  ;; are never given out to more than one slave.
  ;;
  ;; NOTE! allocate-tasks can dig around in here and find tasks that
  ;; it wishes to send to multiple slaves. In this case, simply a new
  ;; slave entry is added to the speculation structure when the same
  ;; task gets mapped to multiple slaves. Reclamation of task-ids will have
  ;; to be aware that the same task-id might have been given out multiple
  ;; times to be worked upon.
  ;;
  ;; Keyed by task id, Value is mw-speculation structure.
  (speculations (make-hash-table :test #'equal))

  ;; For whatever reason if any tasks are discovered as unrunnable,
  ;; their actual task structures are removed from the other fields
  ;; and placed here. Eventually, all of these task structures (and
  ;; their ownership) will be given back to the master function to do
  ;; with as it pleases. This is simply a list upon which we push
  ;; unrunnable tasks.
  (unrunnable-tasks nil)

  ;; How many tasks that we stated we didn't want to retry, and were actually
  ;; thrown away.
  (num-no-retry-discards 0)

  ;; Here we keep track of various target-numbers for the various
  ;; known algorithms, and the general one too.
  (target-numbers (make-mw-target-number))
  )


