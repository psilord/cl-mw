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

(in-package :cl-mw.examples.argument-processing)

;; Used for REPL testing purposes to make things easier to type.
(defun mw-master ()
  (mw-initialize '("--mw-master" "--mw-slave-task-group" "100"
                   "--mw-master-host" "localhost" "--mw-resource-file" "zzz.txt"
                   "--mw-slave-result-group" "100")))

(defun mw-slave (port)
  (mw-initialize `("--mw-slave" "--mw-master-host" "localhost"
                                "--mw-master-port"
                                ,(format nil "~D" port))))


(define-mw-algorithm required-parameters (a b c)
  (list :required a b c))

(define-mw-algorithm optional-parameters (a b c &optional d (e 42))
  (list :optional a b c d e))

(define-mw-algorithm keyword-simple-parameters (a b c &key (d 100) e f)
  (list :keyword-simple a b c d e f))

(define-mw-algorithm keyword-complex-parameters (a b c &key ((:d real-d) 200) e f)
  (list :keyword-complex a b c real-d e f))

(define-mw-algorithm rest-parameters (a b c &rest d)
  (list :rest a b c d))

;; When mw is initialized and it knows this is the master, run this entry
;; point, which will manage the processing loop.
(define-mw-master (argv)
    (declare (ignorable argv))
    (unwind-protect
         (let ((num-required-tasks 1)
		       (num-optional-tasks 1)
		       (num-keyword-simple-tasks 1)
		       (num-keyword-complex-tasks 1)
		       (num-rest-tasks 1)
               (num-results 0))

           ;; queue up some required-parameter tasks to do.
           (dotimes (x num-required-tasks)
             (let ((str (format nil "Required Task: ~A" x)))
               (mw-funcall-required-parameters (1 2 3))))

           ;; queue up some optional-parameter tasks to do.
           (dotimes (x num-optional-tasks)
             (let ((str (format nil "Optional Task: ~A" x)))
               (mw-funcall-optional-parameters (4 5 6 7))))

           ;; queue up some keyword-parameter tasks to do.
           (dotimes (x num-keyword-simple-tasks)
             (let ((str (format nil "Keyword Simple Task: ~A" x)))
               (mw-funcall-keyword-simple-parameters (10 20 30 :f 50 :e 40))))

           ;; queue up some keyword-parameter tasks to do.
           (dotimes (x num-keyword-complex-tasks)
             (let ((str (format nil "Keyword Complex Task: ~A" x)))
               (mw-funcall-keyword-complex-parameters (20 30 40 :f 60 :e 50))))

           ;; queue up some rest-parameter tasks to do.
           (dotimes (x num-rest-tasks)
             (let ((str (format nil "Rest Task: ~A" x)))
               (mw-funcall-rest-parameters (100 102 103 104 105 106 107))))

           ;; While we have some outstanding results to compute...
           (while (/= num-results (+ num-required-tasks num-optional-tasks
		                             num-keyword-simple-tasks 
									 num-keyword-complex-tasks num-rest-tasks))

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

