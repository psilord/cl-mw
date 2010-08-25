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

;; A simple but efficient queue implementation from "ANSI Common Lisp"
;; by Paul Graham.
(defun make-queue ()
  (cons nil nil))

(defun enqueue (obj q)
  (if (null (car q))
      (setf (cdr q) (setf (car q) (list obj)))
      (setf (cdr (cdr q)) (list obj)
            (cdr q) (cdr (cdr q))))
  (car q))

;; For our use of queue, NIL will never be an element in the queue, so
;; when I see nil, I know there is nothing in the queue.
(defun peek-queue (q)
  (car (car q)))

(defun dequeue (q)
  (pop (car q)))

(defun dequeue-n (q n)
  (let ((result nil))
    (do ((i 0 (1+ i)))
        ((or (= i n) (null (peek-queue q))) (nreverse result))
      (push (dequeue q) result))))

(defun empty-queue (q)
  (null (car q)))

(defun length-queue (q)
  (length (car q)))

;; Store open connection information into the open connections db
;; XXX figure out why equalp doesn't work with the key `(,who ,port)
(defun save-open-connection (who port client)
  (setf (gethash (format nil "~A:~A" who port) *open-connections*) client))

;; Remove an entry, but don't close, an open connection in the db
;; XXX figure out why equalp doesn't work with the key `(,who ,port)
(defun remove-open-connection (who port)
  (remhash (format nil "~A:~A" who port) *open-connections*))

;; Dump all open connections in the db for debugging purposes.
(defun dump-open-connections (&key (a-stream *standard-output*))
  (emit a-stream "Dumping Open Connection Table~%")
  (maphash #'(lambda (who/port client)
               (emit a-stream "    Open Connection: ~S~%" who/port))
           *open-connections*))

;; Close all open connections in the db
(defun close-open-connections ()
  (maphash #'(lambda (who/port client)
               (alog t "EOF" "FORCE -> ~A~%" who/port)
               (close client :abort t))
           *open-connections*))


;; The master main entrance function
(defmacro define-mw-master (args &body body)
  (with-unique-names ((master-func "MASTER-FUNCTION-"))
    `(progn
       (defun ,(intern (symbol-name master-func)) (,@args)
         ,@body)
       (setf *master-function*
             (function ,(intern (symbol-name master-func)))))))

;; The slave main entrance function
(defmacro define-mw-slave (args &body body)
  (with-unique-names ((slave-func "SLAVE-FUNCTION-"))
    `(progn
       (defun ,(intern (symbol-name slave-func)) (,@args)
         ,@body)
       (setf *slave-function*
             (function ,(intern (symbol-name slave-func)))))))

;; Returns the current version of the library as a string.
(defun mw-version-string ()
  "0.1")

;; All exit paths from a slave algorithm should exit with this which
;; becomes the answer back to the master.
(defmacro mw-result (form)
  `(values :result (serialize ,form)))

;; A function used to get all of the symbol names from a quoted
;; argument list to a function so I can do manipulation with them.
;; It is used in the define-mw-algorithm macro.
(defun extract-arg-names (arg-list)
  (cond
    ((null arg-list)
     nil)

    ;; skip the usual parameter keywords
    ;; XXX This feature is broken right now.
    ((or (eq '&key (car arg-list))
         (eq '&rest (car arg-list))
         (eq '&optional (car arg-list)))
     (extract-arg-names (cdr arg-list)))

    ;; If I'm processing a list parameter, the first entry is the symbol
    ;; name in which I am interested.
    ;; XXX This feature is broken right now.
    ((listp (car arg-list))
     (cons (caar arg-list) (extract-arg-names (cdr arg-list))))

    ;; Otherwise, just copy the argument
    (t
     (cons (car arg-list) (extract-arg-names (cdr arg-list))))))

;; A small utility to help me make new symbols for created
;; functions...  Concatenate strings and symbols in order in rest into
;; an uppercase interned symbol.
(defun make-interned-sym (&rest pieces)
  (flet ((->string (x)
           ;; Convert x into a string representation, but be clever about it.
           (cond
             ((symbolp x)
              (symbol-name x))
             ((stringp x)
              x)
             (t
              (format nil "~S" x)))))
    ;; Join the stringified pieces together and return the newly
    ;; interned symbol
    (intern
     (string-upcase
      (reduce
       #'(lambda (&optional a b)
           (concatenate 'string a  b))
       (mapcar #'->string pieces))))))

(defmacro define-mw-algorithm (name args &rest body)
  ;; XXX Since this macro generates functions into the application
  ;; package which ultimately need to call into deeper interfaces in
  ;; the MW package than I'm willing to export, I'll use the cl-mw::
  ;; package prefix for now when calling into some of the mw library.
  (with-unique-names (sid tag do-it-anyway retry)
    `(progn
       ;; ;;;;;;;;;;;;;
       ;; generate the user's supplied algorithm function
       ;; This is the function that will accept the user task for processing.
       ;; ;;;;;;;;;;;;;
       (defun ,name ,args
         ;; We take whatever is the user's result and package it up.
         ;; We also make a block of the same name so the user can't return
         ;; from the function and somehow escape the mw-result wrapper.
         (cl-mw::mw-result (block ,name ,@body)))

       ;; ;;;;;;;;;;;;;
       ;; This allows one to set a target number for a specific algorithm. A
       ;; Target number is how many tasks the master algorithm would like to
       ;; keep in memory at one time.
       ;; ;;;;;;;;;;;;;
       (defun ,(make-interned-sym "mw-set-target-number-for-" name)
           (level)
         (with-slots (target-numbers) *taskjar*
           (with-slots (algorithm-target-numbers) target-numbers
             (cl-mw::skvh ,(symbol-name name) level algorithm-target-numbers))))

       ;; ;;;;;;;;;;;;;
       ;; This allows one to get the target number for a specific algorithm
       ;; ;;;;;;;;;;;;;
       (defun ,(make-interned-sym "mw-get-target-number-for-" name)
           ()
         (with-slots (target-numbers) *taskjar*
           (with-slots (algorithm-target-numbers) target-numbers
             (multiple-value-bind (level present)
                 (cl-mw::lkh ,(symbol-name name) algorithm-target-numbers)
               (if present
                   level
                   (cl-mw::skvh ,(symbol-name name) 0 algorithm-target-numbers))))))


       ;; ;;;;;;;;;;;;;
       ;; This allows one to figure out how many tasks are pending in the MW
       ;; system for a specific algorithm
       ;; ;;;;;;;;;;;;;
       (defun ,(make-interned-sym "mw-pending-tasks-for-" name)
           ()
         (with-slots (target-numbers) *taskjar*
           (with-slots (algorithm-pending-tasks) target-numbers
             (multiple-value-bind (val present)
                 (cl-mw::lkh ,(symbol-name name) algorithm-pending-tasks)
               (if present
                   val
                   (cl-mw::skvh ,(symbol-name name) 0 algorithm-pending-tasks))))))

       ;; ;;;;;;;;;;;;;
       ;; This allows one to learn how many tasks would I need to submit into
       ;; the system for this algorithm to reach the target-number for it.
       ;; ;;;;;;;;;;;;;
       (defun ,(make-interned-sym "mw-upto-target-number-" name)
           ()
         (with-slots (target-numbers) *taskjar*
           (with-slots (algorithm-target-numbers algorithm-pending-tasks)
               target-numbers
             (mw-zero-clamp
              (-
               (,(make-interned-sym "mw-get-target-number-for-" name))
               (,(make-interned-sym "mw-pending-tasks-for-" name)))))))

       ;; ;;;;;;;;;;;;;
       ;; generate the macro to queue up tasks for this slave
       ;; It'll be named MW-FUNCALL-name all uppercase
       ;; ;;;;;;;;;;;;;
       (defmacro ,(make-interned-sym "mw-funcall-" name)

           ;; Argument list for macro. Notice that it will use
           ;; destructuring to create the exact same signature as the
           ;; generated function for the first list of parameters. The
           ;; stuff after the list are keyed parameters that the macro
           ;; itself uses. So, anything passed inside of the ... in
           ;; (mw-funcall-xxx (...)) can match the function and will
           ;; be serialized properly. Anything after the (...) are
           ;; keyword arguments for the mw-funcall-* macro.
           ;;
           ;; XXX For now, don't use keyword or other types of arguments in the
           ;; mw-funcall-* function. I need to figure those out better.
           (,args &key sid tag do-it-anyway (retry t))

         ;; Add the new task to the taskjar. Under certain conditions, like I'm
         ;; adding a do-it-anyway nil task for a slave which doesn't exist,
         ;; the task might end up immediately in the unrunnable set.
         (let ((s (gensym))
               (dia (gensym)))
           `(let ((,s ,sid)
                  (,dia ,do-it-anyway))
              (add-task (make-mw-task
                         :pkge (package-name (symbol-package ',',name))
                         :algorithm ,',(string-upcase (symbol-name name))
                         :sid ,s
                         :tag ,tag
                         ;; If we don't supply a sid, then do-it-anyway will be
                         ;; true, otherwise it is false unless we also specify
                         ;; do-it-anyway with a sid.
                         :do-it-anyway (if ,dia
                                           ,dia
                                           (if ,s nil t))
                         :retry ,retry
                         :queue-time (get-universal-time)
                         :packet (serialize (list ,,@(extract-arg-names args)))))))))))

(defun mw-set-target-number (level)
  (with-slots (target-numbers) *taskjar*
    (with-slots (general-target-number) target-numbers
      (setf general-target-number (mw-zero-clamp level)))))

(defun mw-get-target-number ()
  (with-slots (target-numbers) *taskjar*
    (with-slots (general-target-number) target-numbers
      general-target-number)))

;; This is different than mw-num-runnable-tasks since these are ones that
;; are known NOT to be actually running whereas the first is running and not
;; running tasks.
(defun mw-pending-tasks ()
  (with-slots (target-numbers) *taskjar*
    (with-slots (general-pending) target-numbers
      general-pending)))

;; how many tasks do I need to insert to get up to the general
;; target-number?
(defun mw-upto-target-number ()
  (mw-zero-clamp (- (mw-get-target-number)
                    (mw-pending-tasks))))

;; If we want whatever pending results we need, destructively take them
(defun mw-get-results ()
  (let ((results (car (mw-master-results-queue *mwm*))))
    (setf (mw-master-results-queue *mwm*) (make-queue))
    (setf (mw-master-num-results *mwm*) 0)
    results))

;; If we requested any sequential slaves and they have arrived, then give
;; them wholesale to the caller.
(defun mw-get-connected-sequential-slaves ()
  (with-slots (connected-sequential-slaves) *mwm*
    (let ((sids connected-sequential-slaves))
      (setf connected-sequential-slaves nil)
      sids)))

;; If any sequential slaves where known about and happened to have
;; previously disconnected, we allow the master algo to know that.
(defun mw-get-disconnected-sequential-slaves ()
  (with-slots (disconnected-sequential-slaves) *mwm*
    (let ((sids disconnected-sequential-slaves))
      (setf disconnected-sequential-slaves nil)
      sids)))

;; The one and only entrance call into the master or slave. This is
;; what the user should call and it will do the rest.
;;
;; WARNING: Unfortunately, the default keyword argument here is
;; required to be here instead of in the implementation specific lisp file.
(defun mw-initialize (argv &key (system-argv sb-ext:*posix-argv*))
  ;; Neede since a macro is producing references to *debug-stream*
  (declare (special *debug-stream*))
  ;; fill the conf table by parsing the arguments.
  (let ((*conftable* (make-mw-conftable)))
    ;; Set up the conf table which allows me to use the debugging API.
    ;; This will leave the name of the executable in the first
    ;; position, if there is a name. Note: We automatically take into
    ;; consideration the system argv here.
    (let ((argv (mw-parse-argv (concatenate 'list system-argv argv))))
      ;; Either have the debug stream go to stdout, or to a file.
      (with-debug-stream (*debug-stream* (mw-conftable-audit-file *conftable*))
        (let* ((*mwm* (make-mw-master))
               (*mws* (make-mw-worker))
               (*stable* (make-mw-stable))
               (*taskjar* (make-mw-taskjar))
               (*open-connections* (make-hash-table :test #'equalp))
               (*event-base* (make-instance 'event-base)))
          (let ((ret
                 (unwind-protect
                      ;; Next continue on with master or slave initialization
                      ;; which will end up in the actual master or slave
                      ;; algorithm and run it until completion returning its
                      ;; exit form.
                      (with-slots (style member-id) *conftable*
                        (alog t "VERSION" "CL-MW: Version ~A~%"
                              (mw-version-string))
                        (ecase style
                          (:master
                           (alog t "INIT" "MASTER ~S~%" member-id)
                           (mw-master-initialize argv))
                          (:slave
                           (alog t "INIT" "SLAVE ~S~%" member-id)
                           (mw-slave-initialize argv))))

                   ;; Cleanup form after the master or worker finishes
                   (mw-shutdown argv))))

            ;; sanity check the return value to make sure it is legal.
            (if (and (integerp ret)
                     (>= ret 0)
                     (<= ret 255))
                ret
                (progn
                  (alog t "MW-INTERNAL"
                        "Wrong exit code type/value [%S], assuming 255.~%"
                        ret)
                  255))))))))

(defun mw-master-initialize (argv)
  (with-open-socket
      (server :ipv6 nil
              :connect :passive
              :address-family :internet
              :type :stream
              ;; For now, bind to any interface
              :local-host +ipv4-unspecified+
              ;; this forces the bind-address with a random port
              :local-port 0
              ;; this forces the listen-on
              :backlog 50)

    (with-slots (resource-file-update-function computation-status) *mwm*
      ;; Set up the resource file thunk.
      (setf resource-file-update-function (make-resource-file-updater))

      (alog t "MASTER" "READY ~A:~A~%"
            (local-host server) (local-port server))

      ;; For the resource file, now that we know it.
      (setf (mw-conftable-master-port *conftable*) (local-port server))

      ;; register the listener handler that figures out what kind of a
      ;; client connected to the server, either a slave, or a control
      ;; connection.  Once it is figured out, the correct protocol
      ;; handler gets associated with the client connection.
      (set-io-handler *event-base*
                      (socket-os-fd server)
                      :read
                      (make-connection-listener-handler server))

      ;; call the master function, which will drive the event-dispatch
      ;; loop. When this returns, we'll start tearing things down.
      (setf computation-status :in-progress)
      (let ((ret (funcall *master-function* argv)))
        ;; Now that the master has decided things are done (by
        ;; returning), we'll send the shutdown command to all open
        ;; slaves and flush the buffers to them.
        (master-shutdown-slaves)

        ;; If we get here the master algorithm computation is
        ;; finished. We write out the last update to the resource
        ;; file, if any is specified, and this time we mention the
        ;; computation is done. The write is mandatory so we don't get
        ;; tripped up by the "only write the file if it is passed the
        ;; update interval" rule inside of the update function
        ;; closure.
        (setf computation-status :finished)
        (funcall resource-file-update-function :mandatory-write t)

        ret))))

;; Accept the client which just connected to me, and initially assign
;; them the handler which figures out what they are, a control
;; connection, or a client. Once that client figures it out, pass it onto the
;; desired handler.
(defun make-connection-listener-handler (socket)
  (lambda (fd event exception)
    (let ((client (accept-connection socket :wait t)))
      (when client
        (multiple-value-bind (who port)
            (remote-name client)

          (alog t "NEW-CLIENT" "-> ~A:~A~%" who port)

          ;; XXX Not needed in new revision of IOLib, but I'm using an
          ;; older one from the git repository cause it doesn't have
          ;; bugs.
          (setf (iolib.streams:fd-non-blocking client) t)

          ;; save the client's connection so we can close it later if need be
          (save-open-connection who port client)

          ;; We need to disambiguate the client from a control
          ;; connection or a slave connection. So when we make the
          ;; packet-buffer, the notify change function is set to the
          ;; disambiguator function, which will figure out what the
          ;; other side is and create the appropriate structures in
          ;; the appropriate places and assign the next read/write
          ;; function to the packet
          (with-slots (max-read-buffer-size client-timeout) *conftable*
            (let ((pkt-buffer
                   (make-packet-buffer
                    client
                    (make-disconnector client)
                    :max-read-buffer-size max-read-buffer-size)))

              ;; Once this function figures out who is on the other
              ;; side, it'll put the client into the right category of
              ;; either a slave or a control connection.
              (funcall pkt-buffer
                       :read-notify-change-func #'master-disambiguate-client)
              (funcall pkt-buffer
                       :write-notify-change-func #'master-disambiguate-client)
              (set-io-handler *event-base*
                              (socket-os-fd client)
                              :read
                              (funcall pkt-buffer :read-some-bytes)
                              :timeout client-timeout))))))))

;; This expects one packet to be read from the client which describes the
;; client, and then it'll put it where it should go.
(defun master-disambiguate-client (who port client controller cmd &rest args)
  (emit nil
        "[read/write] Disambiguating client from ~A:~A (~S)...~%"
        who port cmd)

  (ecase cmd
    (:read-packet
     ;; We received a complete packet and have the payload. This is where we
     ;; would do the serious checking of what the client is.
     (let ((id-pkt (deserialize (car args))))
       ;; The if is here so I don't get a signal on the destructuring-bind if
       ;; the client is misbehaving.
       (if (and (listp id-pkt) (= (length id-pkt) 2))
           ;; Lesse if the id-pkt has something in it we like.
           (destructuring-bind (query membership) id-pkt
             (if (not (equal membership (mw-conftable-member-id *conftable*)))
                 ;; true
                 (progn
                   (alog t "NEW-CLIENT" "<- EXPECTED MEMBER ~S GOT MEMBER ~S~%"
                         (mw-conftable-member-id *conftable*) membership)
                   (funcall controller :disconnector :remove-all-handlers
                            :close))
                 ;; false
                 (cond
                   ((equal query "SLAVE_READY")
                    ;;(alog t "NEW-CLIENT" "<- SLAVE_READY ~A:~A~%" who port)
                    (let ((new-slave
                           (make-mw-slave :who who :port port
                                          :connected-time (get-universal-time)
                                          :task-group
                                          (mw-conftable-slave-task-group
                                           *conftable*)
                                          :result-group
                                          (mw-conftable-slave-result-group
                                           *conftable*)
                                          :controller controller)))

                      (with-slots (sequential sid) new-slave
                        ;; Figure out if the slave is supposed to be
                        ;; :unordered, :intermingle, or :sequential
                        (assign-slave-allocation new-slave)

                        ;; Put the slave into the stable
                        (add-slave new-slave who port (mw-slave-sid new-slave)
                                   :where :connecting)

                        ;; If we aren't :unordered then make the sid
                        ;; available to the master algorithm for its use.
                        (when (not (eq :unordered sequential))
                          (push sid
                                (mw-master-connected-sequential-slaves *mwm*)))

                        (alog t sid
                              "~A:~A -> [~S] :connecting [~(~S~)]~%"
                              who port membership sequential)

                        ;; When we get network activity associated with
                        ;; this controller, it goes to this function.
                        (funcall controller
                                 :read-notify-change-func
                                 #'master-manage-slave)
                        (funcall controller
                                 :write-notify-change-func
                                 #'master-manage-slave)
                        ;; Tell the slave what its ID is.  Do this
                        ;; AFTER I adjust the notify functions just in
                        ;; case I get a very fast turnaround. I want
                        ;; to make sure the right thing happens.
                        (funcall controller
                                 :writer :send-data (serialize sid)))))

                   ;; XXX TODO. Implement controller codes/client. Currently not
                   ;; implemented and just stubbed out.
                   ((equal query "CONTROL_READY")
                    (alog t "NEW-CLIENT"
                          "<- NOT_IMPL CONTROL_READY [~S] ~A:~A~%"
                          membership who port)
                    (funcall controller :writer :send-data (serialize "OK"))
                    (funcall controller :disconnector :remove-all-handlers
                             :close))

                   (t
                    (alog t "NEW-CLIENT" "<- NOT_IMPL ~A~%" query)
                    (funcall controller :close)))))

           ;; else the id pkt form was bad....
           (progn
             (alog t "NEW-CLIENT <- BAD_IDENT: ~S~%" id-pkt)
             (funcall controller :disconnector :remove-all-handlers :close))))

     :continue)

    (:read-timeout
     ;; If we see this here, then the client didn't ackowledge our
     ;; identification, so we disconnect it.
     (emit t "RECEIVED READ-TIMEOUT~%")
     ;; Slave is busy, but hasn't given us a result in timeout amount
     ;; of time, We will declare the slave lost and disconnect it.
     (emit t "Disconnecting client ~A:~A~%" who port)
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)

    (:write-finished
     (emit nil "Response sent to ~A:~A correctly!~%" who port)
     :continue)

    (:read-eof
     (alog t "NEW-CLIENT" "<- [~A:~A] CEOF~%" who port)
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)

    (:writes-flushed
     :continue)

    (:write-problem
     (emit t "Client exhibited a writing problem ~S. Closing connection.~%"
           args)
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)

    (:read-problem
     ;; Figure parse out all of the various types of problems and what to do
     ;; about them.
     (emit t
           "Client ~A:~A exhibited a reading problem ~S. Closing connection.~%"
           who port args)
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)))

;; Whenever a slave speaks to us, this is what we say back...
(defun master-manage-slave (who port client controller cmd &rest args)
  (multiple-value-bind (slave status last-heard-from)
      (find-slave :who who :port port)
    (with-slots (sid last-heard-from) slave
      (ecase cmd
        (:read-packet
         ;; This is a complete information packet from the
         ;; slave. It'll often contain results or something else the
         ;; slave wants to tell us.  we ignore anything from a slave
         ;; when it is marked :shutting-down
         (unless (eq status :shutting-down)

           ;; master-process-slave-packet will return true if it liked
           ;; the packet and false if it didn't. If it didn't, we cut
           ;; the connection to the slave since it was misbehaving.
           (unless (master-process-slave-packet slave (deserialize (car args)))

             ;; The master didn't like what the slave had to say, and
             ;; so therefore will be severing the connection to the
             ;; slave. Later, we'll move the tasks back to the
             ;; runnable queue slave or make them unrunnable.
             (alog t sid "STUPID -> :disconnecting~%")
             (move-slave :who who :port port :where :disconnected)
             (funcall controller :disconnector :remove-all-handlers :close)))
         :continue)

        (:read-timeout
         ;; If we get this it means the slave hasn't written anything to us in
         ;; a while. So we'll check to see if the slave is :busy and if so,
         ;; when was the last time we got a result from it.
         (when (and (eq status :busy)
                    (>= (- (get-universal-time) last-heard-from)
                        (mw-conftable-client-timeout *conftable*)))
           ;; Slave is busy, but hasn't given us a result in timeout amount
           ;; of time, We will declare the slave lost and disconnect it.
           (alog t sid "TIMEOUT -> :disconnected~%")
           (move-slave :who who :port port :where :disconnected)
           (funcall controller :disconnector :remove-all-handlers :close))
         :abort)

        (:read-eof
         (alog t sid "CEOF -> :disconnected~%")
         (move-slave :who who :port port :where :disconnected)
         (funcall controller :disconnector :remove-all-handlers :close)
         :abort)

        ((:read-problem :write-problem)
         ;; Slave had a nasty problem, we'll mark it disconnected and
         ;; let it be cleaned up later.
         (alog t sid "PROBLEM -> :disconnecting ~S~%" args)

         (move-slave :who who :port port :where :disconnected)
         (funcall controller :disconnector :remove-all-handlers :close)
         :abort)

        (:write-finished
         (emit nil "Finished writing to slave: [~A:~A]~%" who port)
         (ecase status
           (:connecting
            (alog t sid "-> :idle~%")
            (move-slave :who who :port port :where :idle))
           (:idle
            (alog t sid "WHY -> :idle~%"))
           (:busy
            (alog t sid "-> :busy~%"))
           (:shutting-down
            (alog t sid "-> :shutting-down~%" who port))
           (:disconnected
            (alog t sid "WHY -> :disconnected~%")))
         :continue)

        (:writes-flushed
         ;; If I flushed communication to a slave and it is in
         ;; shutting-down mode, then finish the job and close the
         ;; connection to the slave.
         (when (eq status :shutting-down)
           (alog t sid "-> :disconnected~%")
           (move-slave :who who :port port :where :disconnected)
           (funcall controller :disconnector :remove-all-handlers :close))

         :continue)))))

;; Here we figure out what the packet was the slave sent us and send it on its
;; way down the control flow to the right spot.
(defun master-process-slave-packet (slave data)
  (destructuring-bind (cmd results) data
    (ecase cmd
      (:finished-results
       ;; Process each result from the slave.
       (with-slots (sid who port pending-task-queue) slave
         (alog t sid "-> ~A results~%" (length results))

         ;; return true if every task processed in this loop was
         ;; correctly found where it was supposed to be found in the
         ;; pending task queue for the slave
         (every
          #'identity
          (mapcar
           #'(lambda (result)
               (destructuring-bind (task-id algo-action algo-result) result
                 (let ((queued-task-id (peek-queue pending-task-queue))
                       (result-form (deserialize algo-result)))
                   (if (not (equal task-id queued-task-id))
                       (progn
                         (alog t sid
                               "UNEXPECT ~A EXPECT ~A~%"
                               task-id queued-task-id)
                         nil)
                       (progn
                         (alog nil sid
                               "EXPECT ~A EXPECT ~A~%" task-id queued-task-id)
                         ;; This actually dequeues the task we peeked
                         ;; earlier and makes it so when we peek again
                         ;; as the mapcar moves left to right on the
                         ;; list, we see the next correct task
                         (master-accept-slave-result slave task-id algo-action
                                                     result-form)
                         t)))))
           results)))))))

;; If the slave sent us back a finished-result, then here we process
;; it into an actual result if there isn't already a result for it or
;; ignore the result if there already was.
(defun master-accept-slave-result (slave rtask-id algo-action result-form)
  (with-slots (sid who port pending-task-queue num-pending-tasks
                   num-good-speculations num-bad-speculations
                   last-heard-from) slave

    (let ((task-id (dequeue pending-task-queue)))
      (emit nil "accepting result ~A~%" task-id)
      (assert (equal rtask-id task-id))

      ;; If this is the first result for this task, then make a formal result
      ;; structure and store it to give it back to the master function later.
      (if (resolve-speculation slave task-id t)
          (progn
            ;; The task structure is ultimately retrieved out of the
            ;; taskjar and left off to be garbage collected here.
            (let ((task (remove-task task-id)))
              (incf num-good-speculations)
              (enqueue
               (make-mw-result :tid task-id
                               :algorithm algo-action
                               :sid sid
                               :tag (mw-task-tag task)
                               :compute-time (- (get-universal-time)
                                                (mw-task-sent-time task))
                               :packet result-form)
               (mw-master-results-queue *mwm*))
              (incf (mw-master-num-results *mwm*))))
          ;; oops, already had computed a result, we speculated wrong!
          (incf num-bad-speculations))

      ;; don't forget to tally what the slave did
      (decf num-pending-tasks)

      ;; Here we record when we last heard from this slave with
      ;; respect to results. This is used for timeout calculations
      ;; with the slave.
      (setf last-heard-from (get-universal-time))

      (when (zerop num-pending-tasks)
        ;; If the slave processed all of its tasks, then make it idle so it can
        ;; get some more.
        (move-slave :sid sid :where :idle)
        (alog t sid "-> :idle~%")))))

;; What we'll do here is find all of the slaves with a network
;; connection, move them to shutting down, send them a shutdown
;; request, and wait until the manage-slave handler moves all of them
;; to disconnected.
(defun master-shutdown-slaves ()
  ;; Shut down all connected slaves
  (let ((slaves (find-all-slaves :from '(:busy :idle :connecting)))
        (go-away (serialize (list :shutdown))))
    (dolist (slave slaves)
      (with-slots (sid) slave
        (alog t sid "TRY-SHUTDOWN~%"))
      (move-slave :sid (mw-slave-sid slave) :where :shutting-down)
      (funcall (mw-slave-controller slave) :writer :send-data go-away)
      (funcall (mw-slave-controller slave) :writer :flush))

    ;; Now we loop until there are no more connected slaves, driving the i/o
    ;; loop.

    ;; XXX TODO, implement time limit for shutting off slaves
    (do* ((connected '(:busy :idle :connecting :shutting-down))
          (done nil)
          (slaves (find-all-slaves :from connected))
          (find-all-salves :from connected))
         ((null (find-all-slaves :from connected)))
      (event-dispatch *event-base* :timeout .05))

    ))

;; The master will allocate incoming slaves to be :sequential,
;; :intermingle, and then :unordered--using that perference, as
;; desired by the master algorithm.
(defun mw-allocate-slaves (&key (amount 1000) (kind :unordered))
  (assert (member kind '(:unordered :intermingle :sequential)))
  (with-slots (slaves-desired) *mwm*
    (incf (lkh kind slaves-desired) amount)))

;; Control the target numbers for the various groups. This won't get
;; rid of any connected slaves, but it won't replenish them when they
;; disconnect.
(defun mw-deallocate-slaves (&key (amount 0) (kind :unordered))
  (assert (member kind '(:unordered :intermingle :sequential)))
  (unless (<= amount 0)
    (with-slots (slaves-desired) *mwm*
      (decf (lkh kind slaves-desired) amount)
      (when (< (lkh kind slaves-desired) 0)
        (setf (lkh kind slaves-desired) 0)))
    t))


;; Return a number which represents the total number of needed slaves in order
;; to have us reach our maximum desired amount give what we already have.
(defun total-slaves-needed ()
  ;; We can have more slaves than desired in the :unordered category,
  ;; but just in case of somehow getting more slaves in the other
  ;; categories, we're also going to treat them with a zero clamp as
  ;; well. This is to ensure the total slaves needed is zero or
  ;; higher, regardless of how many slaves we need, or have more of
  ;; than we desired.
  (with-slots (slaves-desired slaves-acquired) *mwm*
    (+ (mw-zero-clamp (- (lkh :unordered slaves-desired)
                         (lkh :unordered slaves-acquired)))
       (mw-zero-clamp (- (lkh :intermingle slaves-desired)
                         (lkh :intermingle slaves-acquired)))
       (mw-zero-clamp (- (lkh :sequential slaves-desired)
                         (lkh :sequential slaves-acquired))))))

;; If supplied a valid :slave-sid, then freed :intermingle or
;; :sequential slaves simply get put into the :unordered group and the
;; number desired for that group the slave was originally in goes
;; down.
;;
;; If supplied a :kind, then simply decrement by one the group size
;; associated with that kind. Do no changes otherwise to any current
;; sequential slaves. [This is often done when a disconnected
;; sequential slave happens and you don't want it to be replaced.
;;
;; Returns t if a slave had been present and was freed, nil otherwise.
;;
;; If the master algo frees a slave which was in the middle of
;; processing some sequential tasks, MW will wait until the results come
;; back and give them as expected results to the master algorithm. It is up
;; to the master algorithm to discern if it actually wants those results.
(defun mw-free-slave (&key slave-sid kind)

  ;; I can have one or the other, but not both.
  (assert (and (or slave-sid kind) (not (and slave-sid kind))))

  (if slave-sid
      (when-let ((slave (remove-slave :sid slave-sid)))
        (with-slots (sequential who port sid status) slave

          ;; If the master algo has freed a sequential slave, it means it
          ;; permanently wants one less desired sequential slave.
          (unless (eq :unordered sequential)
            (decf (lkh sequential (mw-master-slaves-desired *mwm*))))

          ;; Don't adjust the sequentialness of a disconnected slave since
          ;; that will impact us in the master-iterate loop and doing our
          ;; bookeeping at that time.
          (unless (eq :disconnected status)
            ;; Adjust the kind of the slave back to unordered
            (setf sequential :unordered)
            ;; Put it back into the stable into the same stall I found it,
            ;; but this will change what group the slave is in.
            (add-slave slave who port sid :where status)
            (alog t sid "-> :unordered~%")
            t)))

      (mw-deallocate-slaves :amount 1 :kind kind)))

;; Determine for which allocation this slave is destined, assign it there, and
;; update the bookeeping in the master structure.
(defun assign-slave-allocation (slave)
  (with-slots (slaves-desired slaves-acquired) *mwm*
    (cond
      ((> (lkh :sequential slaves-desired)
          (lkh :sequential slaves-acquired))
       (setf (mw-slave-sequential slave) :sequential)
       (incf (lkh :sequential slaves-acquired))
       ;; Now put it into the queue for the master algo to receive.

       slave)

      ((> (lkh :intermingle slaves-desired)
          (lkh :intermingle slaves-acquired))
       (setf (mw-slave-sequential slave) :intermingle)
       (incf (lkh :intermingle slaves-acquired))

       ;; Now put it into the queue for the master algo to receive.
       slave)

      (t
       ;; All slaves to go the :unordered group if noone else wants them.
       (setf (mw-slave-sequential slave) :unordered)
       (incf (lkh :unordered slaves-acquired))))))

;; A collection of functions which deal with slave task ordering.
(defun unordered-only-slavep (slave)
  (eq :unordered (mw-slave-sequential slave)))

;; An intermingled slave is both an unordered slave and an ordered slave,
;; depending upon context.
(defun unordered-slavep (slave)
  (with-slots (sequential) slave
    (or (eq :unordered sequential)
        (eq :intermingle sequential))))

;; An intermingled slave is both an unordered slave and an ordered slave,
;; depending upon context.
(defun ordered-slavep (slave)
  (with-slots (sequential) slave
    (or (eq :intermingle sequential)
        (eq :sequential sequential))))

;; A sequential slave is only an ordered slave
(defun ordered-only-slavep (slave)
  (with-slots (sequential) slave
    (eq :sequential sequential)))

(defun mw-master-loop (&key (timeout .05))
  ;; XXX Whoa, this is crappy code. I need a multiple value do...
  (do ((done nil)
       (n 0)
       (p 0)
       (c 0)
       (d 0))
      (done (values n p c d))

    (multiple-value-bind (norun pending con-seq-slaves discon-seq-slaves)
        (mw-master-loop-iterate :timeout timeout)

      ;; If some slave results or other actionable things have shown
      ;; up from slaves then consider ourselves done. This allows the
      ;; master to process the results, and call back into this
      ;; function in order to keep searching for the right result.
      (when (or (> norun 0)
                (> pending 0)
                (> con-seq-slaves 0)
                (> discon-seq-slaves 0))
        (setf n norun
              p pending
              c con-seq-slaves
              d discon-seq-slaves
              done t)))))

(defun mw-master-loop-iterate (&key (timeout .05))
  ;; 0. This function can be called when there is already some tasks
  ;; in the ready queue because the master algorithm might have put
  ;; them there

  ;; 1. For each disconnected slave, process the tasks which had been
  ;; owned by the disconnected slave and see if we can reclaim them to
  ;; run on another slave, or make them unrunnable. Then, if the slave
  ;; had been something other than :unordered, shove the sid into the
  ;; master structure so the master algo knows what happened. We then
  ;; drop the removed slaves into the garbage collector.
  (mapc #'(lambda (slave/stall)
            (destructuring-bind (slave stall) slave/stall

              (reclaim-disconnected-tasks slave)

              (with-slots (sequential sid) slave
                (with-slots (disconnected-sequential-slaves
                             slaves-acquired) *mwm*

                  ;; The master algo is probably doing its own
                  ;; bookeeping, and having both the sid and the
                  ;; sequentiality will help it make judgements
                  ;; about the resources it wants.
                  (unless (eq :unordered sequential)
                    (push (list sid sequential)
                          disconnected-sequential-slaves))

                  ;; This allows us to try and replace the lost
                  ;; sequential slave (unless of course the
                  ;; master-algo decreases the number desired in this
                  ;; sequential group). The replacement will happen
                  ;; when we write out our resource-file and a higher
                  ;; level resource manager inspects it.
                  (decf (lkh sequential slaves-acquired))
                  (when (< (lkh sequential slaves-acquired) 0)
                    (setf (lkh sequential slaves-acquired) 0))))))

        (remove-slaves :from :disconnected))

  ;; 2. We check to see if any of the sequential tasks are destined for
  ;; slaves which no longer exist or don't want to run them.
  (reclaim-defunct-sequential-tasks)

  ;; 3. For each idle slave, try to allocate some tasks to it and send it a
  ;; payload of work. The function ALLOCATE-TASKS is smart enough to schedule
  ;; tasks appropriately with regards to the sequential setting of slaves and
  ;; the available task load.
  (mapc #'(lambda (slave)
            (with-slots (who port controller sid result-group) slave
              ;; If allocate-tasks is successful in finding some tasks
              ;; for the slave, it will return true, if so, the slave
              ;; should get the real payload and move to busy.
              (when (allocate-tasks slave)
                (let* ((tasks (realize-allocation slave))
                       (pkt (serialize
                             (list :task-list
                                   result-group
                                   ;; Set the sent-time to now, and
                                   ;; return the list of tasks to be
                                   ;; serialized.  XXX This is broken
                                   ;; since multiple speculations will
                                   ;; reset this value. We probably
                                   ;; have to store the time in the
                                   ;; speculation structure and then
                                   ;; do something meaningful with
                                   ;; them as they come back.
                                   (mapc #'(lambda (tsk)
                                             (setf (mw-task-sent-time tsk)
                                                   (get-universal-time)))
                                         tasks)))))

                  ;; Move the slave to the busy stall
                  (move-slave :who who
                              :port port
                              :where :busy)

                  (alog t sid "<- ~A tasks~%" (length tasks))

                  ;; Send the task data to the slave!
                  (funcall controller :writer :send-data pkt)))))

        ;; The slave list on which we are mapping.
        (find-all-slaves :from :idle))

  ;; 4. Write the resource file to let the driver of the MW system know what
  ;; I as the master actually want.
  ;;
  ;; XXX I kinda put this willy-nilly here, is this the right place in
  ;; the iteration algorithm?
  (funcall (mw-master-resource-file-update-function *mwm*))

  ;; 5. handle all of the ready actions to/from the network. If any
  ;; results come back from any slaves they will be side effected
  ;; into the result queue
  (event-dispatch *event-base* :timeout timeout)

  ;; 6. If any results had come back from the clients, give them to the
  ;; consumer function if available.
  ;; The values result is:
  ;;
  ;; How many unprocessed unrunnable tasks there are.
  ;; How many unprocessed results are present and waiting for consumption.
  ;; How many requested sequential slaves have just showed up.
  ;; How many sequential slaves have just disconnected.

  ;; Nothing to call, just see if there is any results pending.
  (values (length (mw-taskjar-unrunnable-tasks *taskjar*))
          (mw-master-num-results *mwm*)
          (length (mw-master-connected-sequential-slaves *mwm*))
          (length (mw-master-disconnected-sequential-slaves *mwm*))))


(defun mw-slave-initialize (argv)
  (handler-case
      (if (mw-conftable-computation-finished *conftable*)
          ;; If we happened to read a resource file which told us the
          ;; computation is done, then we notice it here and exit _fast_
          ;; with a zero return code.
          0

          ;; Or, we start up the slave like we're supposed to...
          (let* ((remote-host (mw-conftable-master-host *conftable*))
                 (remote-port (mw-conftable-master-port *conftable*))
                 (master (make-socket
                          :ipv6 nil
                          :connect :active
                          :address-family :internet
                          :type :stream
                          :remote-host remote-host
                          :remote-port remote-port)))
            (unwind-protect
                 (progn
                   (alog t "MASTER" "<- CONNECTED TO ~A:~A FROM ~A:~A~%"
                         (remote-host master) (remote-port master)
                         (local-host master) (local-port master))

                   (let ((pkt-buf
                          (make-packet-buffer master
                                              (make-disconnector master))))
                     (funcall pkt-buf
                              :read-notify-change-func #'slave-identify)

                     (funcall pkt-buf
                              :write-notify-change-func #'slave-identify)

                     ;; XXX Not needed in new revision of IOLib, but I'm
                     ;; using an older one from the git repository cause
                     ;; it doesn't have bugs.
                     (setf (iolib.streams:fd-non-blocking master) t)

                     (set-io-handler *event-base*
                                     (socket-os-fd master)
                                     :read
                                     (funcall pkt-buf :read-some-bytes))

                     ;; Tell the master I am awake
                     (funcall pkt-buf :writer
                              :send-data
                              (serialize
                               (list "SLAVE_READY"
                                     (mw-conftable-member-id *conftable*))))

                     ;; return integer of the slave-algo is the return
                     ;; value of the mw library.
                     (funcall *slave-function* argv)))

              ;; Cleanup form
              (close master))))

    ;; Various signaled conditions...
    (socket-connection-refused-error ()
      (with-slots (master-host master-port) *conftable*
        (alog t "MASTER" "<- CONNECT REFUSED ~A:~A~%" master-host master-port)
        ;; exit the process with
        255))))

;; This is the initial communication between the slave and the master such that
;; the master registers the slave and gives it a slave id.
(defun slave-identify (who port client controller cmd &rest args)
  (emit nil
        "[read/write] Identify slave to master ~A:~A (~S)...~%"
        who port cmd)

  (ecase cmd
    (:read-packet
     ;; Got the answer from the master about my identity
     (let ((id (deserialize (car args))))
       (alog t "MASTER" "-> ID ~A~%" id)
       (emit nil "Getting ready to accept incoming work!~%")

       ;; Store off the controller back to the master for later
       (setf (mw-worker-controller *mws*) controller)

       ;; Now set up the handlers for the task pump.
       (funcall controller :read-notify-change-func #'slave-receive-task)
       (funcall controller :write-notify-change-func #'slave-send-result)

       ;; XXX do something else with my identity?
       :continue))

    (:read-eof
     (setf (mw-worker-master-disconnect *mws*) t)
     ;; XXX Right now, when the slave sends a membership value to the master
     ;; that the master doesn't like, it just cuts the connection to the slave
     ;; leaving the slave to the four winds. Should the master send back
     ;; something that states why it doesn't like the slave? Otherwise, looking
     ;; at a slave's audit log makes it hard to figure out in this case.
     (alog t "MASTER" "-> EOF [BAD MEMBERSHIP?]~%")
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)

    (:read-problem
     (alog t "MASTER" "-> READ-PROBLEM ~S~%" args)
     (setf (mw-worker-master-disconnect *mws*) t)
     (funcall controller :disconnector :remove-all-handlers :close))

    (:write-finished
     (emit nil "Finished telling master that I am a slave~%")
     :continue)

    (:writes-flushed
     :continue)

    (:write-problem
     (alog t "MASTER" "-> WRITE-PROBLEM ~S~%" args)
     (setf (mw-worker-master-disconnect *mws*) t)
     (funcall controller :disconnector :remove-all-handlers :close)
     :abort)))

;; This handles the operation of getting a command from the master.
;; Since the slave is single threaded at this point, we will only check
;; back on the master connection at every task completion boundary.
(defun slave-receive-task (who port client controller cmd &rest args)
  (emit nil "Slave receiving an order from the Master!~%")

  (ecase cmd
    (:read-packet
     (slave-process-order (deserialize (car args)) controller)
     :continue)

    (:read-eof
     (alog t "MASTER" "-> CEOF~%")
     ;; XXX Probably should tear everything down and quit when this happens
     ;; because currently I don't have a means for the slave to reconnect to
     ;; a different master.
     (funcall controller :disconnector :remove-all-handlers :close)
     (setf (mw-worker-master-disconnect *mws*) t)
     :abort)

    (:read-problem
     (alog t "MASTER" "-> EOF: ~S~%" args)
     (funcall controller :disconnector :remove-all-handlers :close)
     (setf (mw-worker-master-disconnect *mws*) t)
     :abort)))

;; This figures out what the order is from the master and queues it up
;; to be done
(defun slave-process-order (order controller)
  (let ((cmd (car order)))

    (emit nil "cmd is ~S~%" cmd)
    (ecase cmd
      (:task-list
       (destructuring-bind (cmd result-grouping payload) order
         (emit nil "Master requests result-grouping of ~A~%" result-grouping)

         ;; XXX make mlog for when talking to the master
         (alog t "MASTER" "-> ~A tasks (~A grouping)~%"
               (length payload) result-grouping)

         (dolist (task payload)
           (emit nil "Enqueueing task-id ~S~%" (mw-task-id task))
           (enqueue (list controller task) (mw-worker-task-queue *mws*)))
         ;; How many results does the master want back in one shot?
         (setf (mw-worker-result-grouping *mws*) result-grouping)))

      (:shutdown
       ;; We *specifically* know the master wanted us to go away since
       ;; it told us at the protocol level.
       (setf (mw-worker-explicit-shutdown *mws*) t)
       (alog t "MASTER" "-> SHUTDOWN~%")))))

;; actually compute the result and queue it up for the return journey
(defun slave-compute-task-and-queue-result (quanta)
  (destructuring-bind (controller task) quanta
    (let* ((algorithm (mw-task-algorithm task))
           (pkge (mw-task-pkge task))
           (algo-func (intern algorithm pkge)))

      (emit nil "Processing task: ~A~%" (mw-task-id task))
      (multiple-value-bind (action result)
          ;; This is the function which actually applies the slave
          ;; algorithm to the task.
          (apply (symbol-function algo-func)
                 (deserialize (mw-task-packet task)))

        ;; Queue up the finished result.
        (enqueue (list (mw-task-id task) action result)
                 (mw-worker-result-queue *mws*))
        (incf (mw-worker-num-results *mws*))

        ;; bookeep it!
        (incf (mw-worker-total-results-completed *mws*)))

      ;; used by the caller to know where to send the task.
      controller)))

(defun slave-send-result (who port client controller cmd &rest args)
  (emit nil "Slave confirmed sending task back to master!~%")

  (ecase cmd
    (:write-finished
     (emit nil "Finished writing result to master!~%")
     :continue)

    (:writes-flushed
     :continue)

    (:write-problem
     (setf (mw-worker-master-disconnect *mws*) t)
     :abort)))

;; A very simple slave loop form.
(defun mw-slave-loop-simple (&key (timeout .05))
  ;; process tasks until for whatever reason it is time to stop.
  (let ((done nil))
    (while (not done)
      (multiple-value-bind (master-disconnect
                            explicit-shutdown
                            total-results-completed
                            num-pending-tasks num-pending-results
                            result-grouping)

          ;; Here I've chosen to process all tasks before checking for
          ;; network I/O.
          (mw-slave-loop :timeout timeout)

        (when master-disconnect
          (setf done t))))

    ;; If we specifically knew the master shut us down, as opposed to just
    ;; disconnecting, then exit properly.
    (if (mw-worker-explicit-shutdown *mws*)
        0
        255)))

;; Process all available tasks, then return a set of values. More efficient
;; from a network point of view.
;;
;; shutdown total-results-completed tasks-pending num-results result-grouping
(defun mw-slave-loop (&key (timeout .05))
  (with-slots (task-queue result-queue result-grouping
                          num-results controller master-disconnect
                          explicit-shutdown
                          total-results-completed) *mws*

    ;; The slave works in result-grouping quanta, not hearing anything the
    ;; master says or going back to the user-code until the amount of
    ;; work needing to be computed is finished.

    ;; Convert the number of tasks the slave is expected to return into
    ;; queued results.
    (do ((count 0 (1+ count)))
        ((or (empty-queue task-queue)
             (= count result-grouping)))

      (when-let ((quanta (dequeue task-queue )))
        (slave-compute-task-and-queue-result quanta)))

    ;; If enough results have been queued OR the task-queue is empty,
    ;; then send back a packet of results.
    (when-let ((results (dequeue-n result-queue result-grouping)))
      (decf num-results (length results))

      (alog t "MASTER" "<- ~A results~%" (length results))

      ;; pack the whole thing together and fire it back to the master.
      (funcall controller :writer
               :send-data (serialize
                           (list :finished-results
                                 ;; results is a list already
                                 results))))

    ;; Check to see if anything new from the master has arrived
    (event-dispatch *event-base* :timeout timeout)

    ;; return some values to the caller.
    (values master-disconnect explicit-shutdown total-results-completed
            (length-queue task-queue) num-results result-grouping)))

;; Process one task, check for network i/o or send completed data, and return.
;; This is much more cpu intensive than the loop version which processes all
;; tasks before checking for network I/O.
(defun mw-slave-loop-iterate (&key (timeout .0001))
  (with-slots (task-queue result-queue result-grouping
                          num-results controller master-disconnect
                          explicit-shutdown
                          total-results-completed) *mws*

    ;; Process exactly one task (if any) and queue (via side-effect) up result.
    (when-let ((quanta (dequeue task-queue)))
      (slave-compute-task-and-queue-result quanta))

    ;; If there are enough tasks to send back, or my task queue is empty, send
    ;; back a grouping of results.
    (when (or (zerop (length-queue task-queue))
              (>= (length-queue result-queue) result-grouping))

      (when-let ((results (dequeue-n result-queue result-grouping)))
        (decf num-results (length results))
        (alog t "MASTER" "<- ~D results~%" (length results))

        (funcall controller :writer
                 :send-data (serialize
                             (list :finished-results
                                   ;; results is a list already
                                   results)))))

    ;; This will always be a nasty polling-like interface.
    (event-dispatch *event-base* :timeout timeout)

    ;; Return the state of the system after the callbacks have been
    ;; called! This could mean that even though we processed all of
    ;; the tasks, we could have gotten more from the master by the
    ;; time we return this set of values.
    (values master-disconnect explicit-shutdown total-results-completed
            (length-queue task-queue) num-results result-grouping)))


(defun make-disconnector (socket)
  (let ((who (remote-host socket))    ; keep track of who this is from
        (port (remote-port socket)))
    ;; We can be told to shutdown a particular side of the connection,
    ;; remove various handlers, or close the connection. Some of these
    ;; tasks are contraindicative of other ones, so there is some
    ;; simple heuristic to get it right.
    #'(lambda (&rest events)
        (let ((fd (socket-os-fd socket)))

          (when (member :shutdown-read events)
            (alog t "EOF" "READ -> ~A:~A~%" who port)
            (shutdown socket :read t))

          (when (member :shutdown-write events)
            (alog t "EOF" "WRITE -> ~A:~A~%" who port)
            (shutdown socket :write t))

          (if (member :remove-all-handlers events)
              (progn
                (emit nil "Removing all handlers for ~A:~A~%" who port)
                (remove-fd-handlers *event-base* fd :read t :write t :error t))
              (progn
                (when (member :read events)
                  (emit nil "Removing read handler for ~A:~A~%" who port)
                  (remove-fd-handlers *event-base* fd :read t))
                (when (member :write events)
                  (emit nil "Removing write handler for ~A:~A~%" who port)
                  (remove-fd-handlers *event-base* fd :write t))
                (when (member :error events)
                  (emit nil "Removing error handler for ~A:~A~%" who port)
                  (remove-fd-handlers *event-base* fd :error t))))

          ;; and finally if were asked to close the socket, we do so here.
          ;; we allow the shutting down of a side and then closing, but in
          ;; practice that should happen rarely or never.
          (when (member :close events)
            (alog t "EOF" "-> ~A:~A~%" who port)
            (close socket)
            (remove-open-connection who port))))))


(defun mw-shutdown (argv)
  ;; NOTE: If any error happens during this call, it is snuffed out by
  ;; whatever was returned from the master-algo function.
  (close-open-connections)
  (when *event-base*
    (close *event-base*))
  (with-slots (style member-id computation-finished) *conftable*
    (if (eq style :master)
        (alog t "FINI" "SHUTDOWN ~S~%" member-id)
        (with-slots (explicit-shutdown) *mws*
          (if (null explicit-shutdown)
              (if computation-finished
                  (alog t "FINI" "COMPUTATION-FINISHED ~S~%" member-id)
                  (alog t "FINI" "MASTER-DISCONNECT ~S~%" member-id))
              (alog t "FINI" "MASTER-SHUTDOWN ~S~%" member-id))))))

;; Return a thunk that when invoked, writes the needed information
;; into the resource file only when it needs to be written. This
;; allows me to call the thunk in the master-iterate function at
;; a reasonably high rate of speed but only actually dump the
;; information out at the right frequency. This is useful for
;; a batch system which is driving the slave pool.
(defun make-resource-file-updater ()
  (with-slots (resource-file member-id master-host master-port
                             resource-file-update-interval slave-executable
                             shared-libs)
      *conftable*
    (let ((update-time 0))
      ;; Unless we mandatorily want to write the file, we only write it when
      ;; we've passed the update time.
      #'(lambda (&key (mandatory-write nil))
          ;; If I don't have a resource file, then make this cheap to
          ;; exit quickly!
          (when resource-file
            (let ((now (get-universal-time)))
              (when (or (>= now update-time)
                        mandatory-write)
                ;; Write out the information into the specified file
                ;; as a tmp file, then rename it to be the right
                ;; one.
                (let ((fname (pathname
                              (concatenate 'string resource-file ".tmp"))))
                  (with-open-file (fout fname
                                        :direction :output
                                        :if-exists :supersede
                                        :if-does-not-exist :create)

                    ;; Stupidly, and in a most non-lisp fashion,
                    ;; emit the file.
                    (format fout ";; Status of the computation~%")
                    (format fout "(:computation-status ~(~S~))~%"
                            (mw-master-computation-status *mwm*))
                    (format fout ";; Time Stamp of Resource File~%")
                    (format fout "(:timestamp ~A)~%" (get-universal-time))
                    (format fout ";; Member ID~%")
                    (format fout "(:member-id ~S)~%" member-id)
                    (format fout ";; Update Interval (sec) of Resource File~%")
                    (format fout "(:update-interval ~A)~%"
                            resource-file-update-interval)
                    (format fout ";; Slaves Needed~%")
                    (format fout "(:slaves-needed ~A)~%"
                            (total-slaves-needed))
                    (format fout ";; Slave Executable and Shared Libs~%")
                    (format fout "(:slave-executable (~S ~S))~%"
                            slave-executable
                            ;; This nest one is a list....
                            shared-libs)
                    (format fout ";; Slave Arguments~%")
                    (format fout "(:slave-arguments (~A))~%"
                            (format nil "~{~S~^ ~}"
                                    `("--mw-slave"
                                      "--mw-master-host"
                                      ,(format nil "~A" master-host)
                                      "--mw-master-port"
                                      ,(format nil "~A" master-port)))))

                  (rename-file fname resource-file)

                  ;; Set the next update time.
                  (setf update-time (+ now resource-file-update-interval))))))))))


;; Recurse down the argv ripping out anything mw related and return an argv
;; with the mw stuff removed.
(defun mw-parse-argv (argv)
  (with-slots (style master-host master-port max-write-buffer-size
                     max-read-buffer-size client-timeout
                     audit-file resource-file resource-file-update-interval
                     slave-task-group slave-result-group member-id
                     slave-executable)
      *conftable*

    ;; If --mw-version-string is present, then emit the version and quit
    ;; regardless of what else is present.
    (when (member "--mw-version-string" argv :test #'string-equal)
      ;; This will exit.
      (format t "CL-MW: Version ~A~%" (mw-version-string))
      (sb-ext:quit :unix-status 1))

    ;; This function does the work of setting the config options and
    ;; building the new argv list which only contains non-mw stuff in
    ;; the order it was found from left to right. --mw-master or --mw-slave
    ;; MUST be first in the argument list.
    (unless
        (intersection '("--mw-master" "--mw-slave")
                      (list (cadr argv))
                      :test #'string-equal)
      (usage))

    ;; XXX recursive for now, if it becomes a problem I'll convert it to an
    ;; iterative method.
    (labels ((parse-argv (argv)
               (let ((option (car argv)))
                 (cond
                   ((null option)
                    nil)
                   ((string-equal "--mw-help" option)
                    ;; This will exit
                    (usage))
                   ((string-equal "--mw-master" option)
                    (setf style :master)
                    (parse-argv (cdr argv)))
                   ((string-equal "--mw-slave" option)
                    (setf style :slave)
                    (parse-argv (cdr argv)))
                   ((string-equal "--mw-master-host" option)
                    (setf master-host (cadr argv))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-master-port" option)
                    (setf master-port (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-max-write-buffer-size" option)
                    (setf max-write-buffer-size (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-max-read-buffer-size" option)
                    (setf max-read-buffer-size (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-client-timeout" option)
                    (setf client-timeout (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-audit-file" option)
                    (setf audit-file (pathname (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-resource-file" option)
                    (if (eq style :master)
                        (setf resource-file (cadr argv))
                        (slave-load-resource-file (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-resource-file-update-interval" option)
                    (setf resource-file-update-interval
                          (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-slave-task-group" option)
                    (setf slave-task-group (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-slave-result-group" option)
                    (setf slave-result-group (read-from-string (cadr argv)))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-member-id" option)
                    (setf member-id (cadr argv))
                    (parse-argv (cddr argv)))
                   ((string-equal "--mw-slave-executable" option)
                    (setf slave-executable (cadr argv))
                    (parse-argv (cddr argv)))
                   (t
                    ;; Copy over anything I don't understand.
                    (cons (car argv) (parse-argv (cdr argv))))))))

      ;; First we set up the conftable and get the new argument list with all
      ;; of the mw stuff removed.
      (let ((new-argv (parse-argv argv)))
        ;; Now we check to make sure each conftable thing which
        ;; requires setting is actually set correctly. I don't want to
        ;; leave the default in the conftable structure as
        ;; initializers since I want one place where everything is
        ;; set.

        (unless (or (eq style :master) (eq style :slave))
          (error "--mw-master or --mw-slave must be specified"))

        (cond
          ((eq style :slave)
           (when (null master-host)
             (error "When a slave, --mw-master-host must be specified"))
           (when (null master-port)
             (error "When a slave, --mw-master-port must be specified")))
          ((eq style :master)
           (unless (null resource-file)
             (when (null master-host)
               ;; If not specified on the command line, then default
               ;; to my hostname as defined by uname
               (setf master-host (hostname))))))

        ;; If you run close to this number, you'll use a lot of memory
        ;; if you have a lot of clients.
        (when (null max-read-buffer-size)
          (setf max-read-buffer-size (* 1024 128)))

        ;; To be used when the write-buffer understands how to pack small
        ;; pieces of data into a larger array for write effeciency.
        (when (null max-write-buffer-size)
          (setf max-write-buffer-size (* 1024 128)))

        (when (null client-timeout)
          ;; Value in seconds.
          (setf client-timeout 60))

        ;; Stupid defaults, but let the user figure out what they need
        (when (null slave-task-group)
          (setf slave-task-group 1))

        ;; Stupid defaults, but let the user figure out what they need
        (when (null slave-result-group)
          (setf slave-result-group 1))

        ;; Check the boundary cases and defaults for slave-executable.
        (if slave-executable
            (progn
              (setf slave-executable
                    (fully-qualified-and-present-file slave-executable))
              ;; If the conversaion failed, then the user specified a bunk
              ;; filename and we complain.
              (unless slave-executable
                (error "Executable specified with --mw-slave-executable is not present!")))
            ;; Ok, we don't have one, so grab the one from argv[0]. In
            ;; the case of a compiled executable, this is correct, in
            ;; the case of a REPL it isn't really, so if you're
            ;; running the master in a REPL and have specified a
            ;; resource-file but no slave-executable you'll probably
            ;; going to be in for a surprise if you are testing a
            ;; harness for a batch system.
            (progn
              (setf slave-executable
                    (fully-qualified-and-present-file (car argv)))
              ;; If this one fails, then something is terribly wrong!
              (unless slave-executable
                (error "Can not determine slave-executable name, even with system argv!"))))

        ;; 5 Minutes is instantaneous on the grid. :) Value in seconds.
        (when (null resource-file-update-interval)
          (setf resource-file-update-interval 300))

        ;; A simple membership test for any clients speaking to the
        ;; master. This is a weak insecure test and mainly used by the
        ;; batch system maintaining the master/slave group for
        ;; tracking purposes.  It's purpose is to make it harder for
        ;; lots of slaves and masters to get confused with each other
        ;; while all running under the same administrative domain.
        (when (null member-id)
          (setf member-id "default-member-id"))

        new-argv))))
;; Can I get the fully qualified filename of a present file on the
;; system?  Return nil if not, or the namestring of the fully
;; qualified file if yes.
(defun fully-qualified-and-present-file (file)
  (handler-case
      (namestring (truename file))
    (type-error () nil)
    (file-error () nil)))

;; A slave can load the master host, master port, and member-id from a
;; resource file. This allows us to easily configure a slave in
;; certain contexts when dealing with batch systems.
(defun slave-load-resource-file (file)
  (with-slots (master-host master-port member-id computation-finished)
      *conftable*
    (with-open-file (fin file :direction :input :if-does-not-exist nil)
      (handler-case
          (progn
            (when (null fin)
              (with-debug-stream (*debug-stream* nil)
                (alog t "CL-MW"
                      "WARNING: Resource file ~A not found. Continuing anyway hoping other command line arguments are present.~%"
                      file))
              (error 'end-of-file))

            (do ((form (read fin) (read fin)))
                (nil)
              (when (listp form)
                (cond
                  ;; If the computation is finished, we immediately
                  ;; turn around and mark ourselves with that
                  ;; knowledge. This causes a fast exit in the slave
                  ;; initialization code.
                  ((eq (car form) :computation-status)
                   (when (eq (cadr form) :finished)
                     (setf computation-finished t)))

                  ((eq (car form) :member-id)
                   (setf member-id (cadr form)))

                  ((eq (car form) :slave-arguments)
                   (setf master-host (cadr (member "--mw-master-host"
                                                   (cadr form)
                                                   :test #'string-equal)))

                   (setf master-port (read-from-string
                                      (cadr (member "--mw-master-port"
                                                    (cadr form)
                                                    :test #'string-equal)))))))))
        (end-of-file ()
          ;; Do nothing
          )))))


(defun usage (&key (exit t))
  (format t
          "Usage:
--mw-help
    Emit the usage and quit.
--mw-version-string
    Emit the version number of the CL-MW library and quit.
--mw-master
    Run the executable in Master Mode. Required if --mw-slave is not set and
    must be first on the command line.
--mw-slave
    Run the executable in Slave Mode. Required if --mw-master is not set and
    must be first on the command line.
--mw-master-host <ip address or hostname>
    When in Master Mode, it is the interface to which the master should
    bind and is emitted to the heartbeat file if any such file is written.
    When in Slave Mode, it is the hostname to which the slave process
    should connect and get work.
--mw-master-port <port>
    To which port should the slave connect for work.
--mw-max-write-buffer-size <size in bytes>
    How big the network writing buffer should be before rejecting the write.
--mw-max-read-buffer-size <size in bytes>
    How big the network reading buffer should be before rejecting the read.
--mw-client-timeout <seconds>
    How many seconds should the master wait for a client to respond
    when the master is expecting a response.
--mw-audit-file <filename>
    A file in which the audit trail of the process is stored.
--mw-resource-file <filename>
    When in master mode:
      Describes the resources needed by the master for a higher level batch
      system to honor.
      The information written into this file is:
        The timestamp of when the file was written.
        The member-id of the master group.
        The update-interval of when this file will be written again.
        How many slave processes are needed by the master.
        The full path to the slave executable.
        The complete arguments to the slave in order for it to connect to
          the currently running master process which produced this file.
      This file is emitted in a lisp friendly format.
    When in slave mode:
      Determine the master-host and master-port to which the slave should
      connect by reading it from the resource file.
      The ordering of this command line option in relation to --mw-master-host
      and --mw-master-port is important. If --mw-master-host and/or
      --mw-master-port are specified before this argument then the
      resource file will overwrite the command line specification, and vice
      versa. If the resource file does not exist, then this is ignored in
      deference to --mw-master-host and --mw-master-port.
--mw-resource-file-update-interval <seconds>
    How many seconds in between updating the resource file with current
      information.
--mw-slave-task-group <positive integer>
    How many tasks should be grouped into a network packet being sent to
    a slave process. If the packet is larger than the maximum size of
    the read buffer of the slave, the slave will abort the read. Defaults
    to 1.
--mw-slave-result-group <positive integer>>
    How many completed results should be grouped into a network packet
    being sent from the slave to the master. If the packet is larger than the
    maximum size of the read buffer for the master, then the master will
    abort the connection to the slave. Defaults to 1.
--mw-member-id <string>
    This is a token which must match between the slave and the master. It is
    used to unsecurely identify a working group pf masters and slave. In a
    harsh environment with many masters and slaves going up and down, this
    acts as a simple sanity check that the correct slaves are connected to the
    correct master process. Default is the string \"default-member-id\".
--mw-slave-executable <path to executable>
    This specifies the fully qualified path to a slave executable. It is used
    when writing the resource file only.
")

  (when exit
    (sb-ext:quit :unix-status 1)))




