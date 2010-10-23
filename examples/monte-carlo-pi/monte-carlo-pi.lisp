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

(in-package :cl-mw.examples.monte-carlo-pi)

;; Used for REPL testing purposes to make things easier to type.
(defun mw-master ()
  (mw-initialize '("--mw-master" "--mw-slave-task-group" "200"
                   "--mw-master-host" "localhost"
                   "--mw-slave-result-group" "200")))

(defun mw-slave (port)
  (mw-initialize `("--mw-slave" "--mw-master-host" "localhost"
                                "--mw-master-port"
                                ,(format nil "~D" port))))


;; Return a list of total and trials in the circle
(define-mw-algorithm monte-carlo-pi (ntrials)
  (labels ((square (x) (* x x))
           (dist (cx cy px py)
             ;; no sqrt because radius is 1!
             (+ (square (- px cx)) (square (- py cy)))))

    (let ((result (list ntrials 0))
          (rstate (make-random-state t)))
      (dotimes (x ntrials)
        ;; we only are using random numbers in quadrant I
        (let ((rx (random 1.0 rstate))
              (ry (random 1.0 rstate)))

          ;; if the random point is in the circle, we record it.
          (when (<= (dist 0 0 rx ry) 1.0)
            (incf (cadr result)))))
      result)))

(defun approximate-pi (num-in-square num-in-circle)
  (* 4.0 (/ num-in-circle num-in-square)))

;; Pick --max-trials <integer> out of the argv, if present, otherwise use
;; default specified.
(defun argv-max-trial-sets (argv default)
  (let ((max-trial-sets (cadr (member "--max-trial-sets" argv :test #'equal))))
    (if (null max-trial-sets)
        default
        (read-from-string max-trial-sets))))

(define-mw-master (argv)
    (unwind-protect
         (progn
           ;; This is the limit of unordered slaves the master will
           ;; tell the higher level layer to gather.
           ;; I don't have to free these, they will go away when the
           ;; computation ends.
           (mw-allocate-slaves :amount 1000 :kind :unordered)
           (mw-set-target-number 10000)

           (let ((max-trial-sets (argv-max-trial-sets argv 100))
                 (sets-submitted 0)
                 (results-received 0)
                 (trial-size (expt 10 7))
                 (num-in-square 0)
                 (num-in-circle 0)
                 (start-time (get-universal-time)))
             ;; Here we produce the tasks on the fly until we produce the
             ;; number of tasks we needed
             (while (< results-received max-trial-sets)

               (format t "Target Number for GENERAL: level=~A pending=~A needs=~A~%"
                       (mw-get-target-number)
                       (mw-pending-tasks)
                       (mw-upto-target-number))

               ;; Inject some tasks into the queue when we have room,
               ;; throttle our injection so we keep a target number of
               ;; tasks in the queue at all times.
               (when (> (mw-upto-target-number) 0)
                 (let* ((num-to-go (- max-trial-sets sets-submitted))
                        (num-fill (min num-to-go (mw-upto-target-number))))

                   (format t "Injecting ~A tasks (out of a known ~A tasks) towards target number ~A..."
                           num-fill num-to-go (mw-get-target-number))

                   (dotimes (x num-fill)
                     (mw-funcall-monte-carlo-pi (trial-size))
                     (incf sets-submitted))

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
                     (incf results-received)
                     (incf num-in-square (car payload))
                     (incf num-in-circle (cadr payload))))))

             ;; Done with the above loop...
             ;; Emit the answer
             (format t "Finished ~A trial sets of ~A size each.~%"
                     max-trial-sets trial-size)
             (format t "Pi[~A, ~A]: ~A~%" num-in-square num-in-circle
                     (approximate-pi num-in-square num-in-circle))

             ;; Emit some statistics.
             (let* ((diff (- (get-universal-time) start-time))
                    (elapsed-time (if (<= diff 0) 1 diff)))
               (format t "This took ~A seconds to compute with ~A tasks per second.~%"
                       elapsed-time
                       (float (/ sets-submitted elapsed-time)))))

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


