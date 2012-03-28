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

(in-package :cl-mw.examples.with-task-policy)

;; Used for REPL testing purposes to make things easier to type.
(defun mw-master ()
  (mw-initialize '("--mw-master" "--mw-slave-task-group" "100"
                   "--mw-master-host" "localhost" "--mw-resource-file" "zzz.txt"
                   "--mw-slave-result-group" "100")))

(defun mw-slave (port)
  (mw-initialize `("--mw-slave" "--mw-master-host" "localhost"
                                "--mw-master-port"
                                ,(format nil "~D" port))))


;; Return whatever normal thing you normally return, but no values!
(define-mw-algorithm times10 (value)
  (* value 10))

;; When mw is initialized and it knows this is the master, run this entry
;; point, which will manage the processing loop.
(define-mw-master (argv)
    (declare (ignorable argv))
  (unwind-protect
       (let ((num-tasks 100)
             (num-results 0))

         ;; Create a function called times10-task which will add tasks
         ;; into the system with the supplied value of :tag being the
         ;; same for all of them. You can specify any task
         ;; policy. Notably, the function is a regular function, so
         ;; you can mapcar it or anything else you'd like. The return
         ;; value is meaningless.
         (mw-with-task-policy-for-times10
            (times10-task :tag 'a-times10-task)
          (mapc #'times10-task
                (loop for i from 1 to num-tasks collecting i)))

         ;; While we have some outstanding results to compute...
         (while (/= num-results num-tasks)

           ;; Drive the system until some results are produced or
           ;; sequential slaves acquired/disconnected.
           (mw-master-loop)

           ;; Process the results, if any
           (when-let ((results (mw-get-results)))
             (dolist (result results)
               (let ((payload (mw-result-packet result)))
                 ;; Keep track of the results so we know when we're done.
                 (incf num-results)

                 (format t "Got result from slave: ~S~%" payload)))))

         ;; exit value for master algo
         0)

    ;; uw-p cleanup form.
    (format t "Master algo cleanup form.~%")))

