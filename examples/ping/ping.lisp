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

(in-package :cl-mw.examples.ping)

;; Used for REPL testing purposes to make things easier to type.
(defun mw-master ()
  (mw-initialize '("--mw-master" "--mw-slave-task-group" "200"
                   "--mw-master-host" "localhost"
                   "--mw-slave-result-group" "200")))

(defun mw-slave (port)
  (mw-initialize `("--mw-slave" "--mw-master-host" "localhost"
                                "--mw-master-port"
                                ,(format nil "~D" port))))


;; Return whatever normal thing you normally return, but no values!
(define-mw-algorithm ping (task)
  (if (equal :ping task)
      :ping-ok
      :ping-not-ok))

;; When mw is initialized and it knows this is the master, run this entry
;; point, which will manage the processing loop.
(define-mw-master (argv)
    (unwind-protect
         (progn
           ;; This is the limit of unordered slaves the master will
           ;; tell the higher level layer to gather.
           ;; I don't have to free these, they will go away when the
           ;; computation ends.
           (mw-allocate-slaves :amount 1000 :kind :unordered)
           (mw-set-target-number 10000)

           (let ((num-pings 1000)
                 (num-pings-submitted 0)
                 (good-pings 0)
                 (bad-pings 0)
                 (start-time (get-universal-time)))
             ;; Here we produce the tasks on the fly until we produce the
             ;; number of tasks we needed
             (while (/= good-pings num-pings)

               (format t "Target Number for GENERAL: level=~A pending=~A needs=~A~%"
                       (mw-get-target-number)
                       (mw-pending-tasks)
                       (mw-upto-target-number))

               ;; Inject some tasks into the queue when we have room,
               ;; throttle our injection so we keep a target number of
               ;; tasks in the queue at all times.
               (when (> (mw-upto-target-number) 0)
                 (let* ((num-to-go (- num-pings num-pings-submitted))
                        (num-fill (min num-to-go (mw-upto-target-number))))

                   (format t "Injecting ~A tasks (out of a known ~A tasks) towards target number ~A..."
                           num-fill num-to-go (mw-get-target-number))

                   (dotimes (x num-fill)
                     (mw-funcall-ping (:ping) :tag num-pings-submitted)
                     (incf num-pings-submitted))

                   (format t "[~A pending]...done!~%" (mw-pending-tasks))))


               ;; Drive the system until some results are produced or
               ;; sequential slaves acquired/disconnected.
               (mw-master-loop)

               ;; Process the unrunnable tasks, if any
               (when-let ((no-run (mw-get-unrunnable-tasks)))
                 (format t "Ignoring ~A unrunnable tasks~%"
                         (length no-run)))

               ;; Process the results, if any
               (when-let ((results (mw-get-results)))
                 (dolist (result results)
                   (let ((payload (mw-result-packet result)))
                     (cond
                       ((equal :ping-ok payload)
                        (incf good-pings))
                       ((equal :ping-not-ok payload)
                        (format t "Got a PING NOT OK!!!~%")
                        (incf bad-pings))
                       (t
                        (format t "Got garbage from MW: ~A~%" payload)))))))

             ;; Done with the above loop...
             ;; Emit some statistics.
             (format t "Got need=~A good=~A bad=~A sub=~A pings.~%"
                     num-pings good-pings bad-pings num-pings-submitted)

             (let* ((diff (- (get-universal-time) start-time))
                    (elapsed-time (if (<= diff 0) 1 diff)))
               (format t "This took ~A seconds to compute with ~A tasks per second.~%"
                       elapsed-time
                       (float (/ num-pings-submitted elapsed-time)))))

           ;; exit value for master algo
           0)

      ;; uw-p cleanup form.
      (format t "Master algo cleanup form.~%")))

;; When mw is initialized and it knows this is a slave, simply start up
;; the slave, connect to the server, and start processing tasks.
(define-mw-slave (argv)
    (unwind-protect
         (mw-slave-loop-simple)

      ;; cleanup form for slave
      (format t "Slave algo cleanup form.~%")))

