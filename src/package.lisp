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

(defpackage #:cl-mw
  (:use #:cl
        #:hu.dwim.serializer
        #:alexandria
        #:iolib
        #:cffi
	#:cl-ppcre)
  (:export
   #:define-mw-master
   #:define-mw-slave
   #:define-mw-algorithm
   #:mw-master-loop
   #:mw-master-loop-iterate
   #:mw-slave-loop
   #:mw-slave-loop-iterate
   #:mw-slave-loop-simple
   #:mw-result-algorithm
   #:mw-result-sid
   #:mw-result-tag
   #:mw-result-compute-time
   #:mw-result-packet
   #:mw-task-sid
   #:mw-task-tag
   #:mw-task-packet
   #:mw-num-runnable-tasks
   #:mw-num-unrunnable-tasks
   #:mw-allocate-slaves
   #:mw-deallocate-slaves
   #:mw-free-slave
   #:mw-get-unrunnable-tasks
   #:mw-get-results
   #:mw-get-connected-ordered-slaves
   #:mw-get-disconnected-ordered-slaves
   #:mw-set-target-number
   #:mw-get-target-number
   #:mw-pending-tasks
   #:mw-upto-target-number
   #:mw-initialize
   #:mw-version-string

   ;; Two useful things often used by mw applications...
   #:mw-zero-clamp
   #:while

   ;; This is how you make the master/slave executable
   #:mw-dump-exec))

(in-package #:cl-mw)

;; TODO SRC for first release.
;;
;; DONE 1. Slave timeouts, fix call-notify-change to use :continue or :abort
;; DONE 2. Configure API for things like buffer sizes and timeouts.
;; DONE 3. Make it so an audit log on disk is made.
;; DONE 4. Solidify audit logs to take predefined stream, default to stdout.
;; DONE 5. Ensure mw-funcall-* interface is good enough for government work.
;; DONE 6. Change 'ordered' to use :unordered instead of nil
;; DONE 7. Finish ordered slave API & boundary conditions with seq slaves.
;; DONE 8. Fix define-mw-algorithm to do argument separation, keep it simple.
;; DONE 9. Implement master's "heartbeat/target number" file.
;; DONE 10. Solidify -loop and -iterate for slave--especially, and master.
;; DONE 11. Fix up save-lisp-and-die & .so library crap to be "passable".
;; DONE 12. Reasonably handle shell/invoker exit values with master/slave algo.
;; DONE 13. Fix up audit log messages to be sane & useful.
;; DONE 14. Implement mw-slave-loop-simple which encapsulates everything!
;; DONE 15. Implement simple concept of a master/client membership set.
;; DONE 16. Fix up exported API to be whatever sane thing it needs to be.
;; DONE 17. Separate test.lisp code and make cl-mw a real lisp library.
;; DONE 18. Fix heartbeat file to be lisp-like and include the slave info.
;; DONE 19. Add low/high target-number API for total tasks and each algorithm.
;; DONE 20. Simple Condor Integration.
;; 21. Massive coverage & boundary testing.

;; FUTURE TODO SRC
;; 0. mw-dump-exec could be told to drop ALL/NONE libraries too...
;; 1. with-checkpointable-bindings & test the hell out of it.
;; 2. Implement allocate-speculation-tasks
;; 3. Implement client authentication/authorization using X509/SSL
;; 4. Implement &rest, &key, &optional, etc with mw-funcall-*
;; 5. Implement a collector to match slaves with masters.
;; 6. Implement that if a master dies, the slave tries to rematch with new one
;; 7. Implement a controller client to give tasks to a master and take results
;; 8. task/result-group should be auto-adjusting based upon slave specified
;;    read/write packet buffer size
;; 9. Very large packet splitting/reassembly through the packet layer.

;; TODO DOCS for first release
;; 1. Write perl scripts to produce statistics and graphs from audit trail.
;; 2. Write manual, include graded examples.

(defvar *debug-header-format* "~2,'0D/~2,'0D/~2,'0D ~2,'0D:~2,'0D:~2,'0D ")

;; When I emit to a nil (as actually stated in the call) stream, turn
;; it all into a progn so I don't pay for the call to format only to
;; throw away the answer (and argument evaluations) when
;; *debug-output* is true.
(defmacro emit (yn fmt &rest args)
  (with-gensyms (sec mi hou day mon yea)
    (if (null yn)
        `(progn)
        `(when *debug-output*
           (multiple-value-bind (,sec ,mi ,hou ,day ,mon ,yea)
               (get-decoded-time)
             (format *debug-stream*
                     (concatenate 'string *debug-header-format* ,fmt)
                     ,mon ,day ,yea ,hou ,mi ,sec
                     ,@args)
             (finish-output *debug-stream*))))))

;; Log something with an audit line. This is usually programmatically parsed
;; out of the debug output to figure out what the system did.
(defmacro alog (yn sid fmt &rest args)
  `(emit ,yn (concatenate 'string "[A] ~A " ,fmt) ,sid ,@args))

;; Either my debug stream is *standard-output*, or it is to a file I
;; specify.  XXX This macro sucks since it copies the body twice which
;; could screw with side-effecting read marcros, rewrite it.
(defmacro with-debug-stream ((str form) &body body)
  (let ((g (gensym)))
    `(let ((,g ,form))
       (if (null ,g)
           (let ((,str *error-output*))
             ,@body)
           (with-open-file (,str ,g
                                 :direction :output
                                 :if-does-not-exist :create
                                 :if-exists :append)
             ,@body)))))

;; a classic macro
(defmacro while (test &body body)
  `(do () ((not ,test)) ,@body))

;; Utility function for my debugging use only
(defun print-hash (ht)
  (maphash #'(lambda (k v) (format t "~s = ~s~%" k v)) ht))

;; Lookup a key in a hash table
(defmacro lkh (key hash)
  `(gethash ,key ,hash))

;; Set the key to be the value in the hash table
(defmacro skvh (key value hash)
  `(setf (lkh ,key ,hash) ,value))

;; Remove an entry from a hash table
(defmacro rkh (key hash)
  `(remhash ,key ,hash))

;; From LOL
(defun group (lst n)
  (when (zerop n) (error "A zero group size is illegal"))
  (labels ((rec (lst acc)
             (let ((rst (nthcdr n lst)))
               (if (consp rst)
                   (rec rst (cons (subseq lst 0 n)
                                  acc))
                   (nreverse
                    (cons lst acc))))))
    (if lst (rec lst nil) nil)))

;; If a value is less than zero, make it zero
(defun mw-zero-clamp (x)
  (if (< x 0) 0 x))

;; Not all of this is used in the MW project, it is here because it is the full
;; utility macro sketch of hash table utilities.
;; Help with making initialized hash tables.
(defun miht-set-key/value (key value ht)
  `(setf (gethash ,key ,ht) ,value))

(defun miht-make-hash-table (&rest args)
  (if (equal args '(nil))
      `(make-hash-table)
      `(make-hash-table ,@args)))

;; use like:
;; (make-initialized-hash-table (:test #'equal) :a 0 :b 1 :c 2 ....)
;; The init-forms or keys/values can be empty if desired.
(defmacro make-initialized-hash-table ((&rest init-form) &body keys/values)
  (let ((h (gensym)))
    `(let ((,h ,(apply #'miht-make-hash-table init-form)))
       ,@(mapcar #'(lambda (key/value)
                     (when (= (length key/value) 1)
                       (error "make-initalized-hash-table: Please supply a value for key ~S"
                              (car key/value)))
                     (destructuring-bind (key value) key/value
                       (miht-set-key/value key value h)))
                 (group keys/values 2))
       ,h)))

;; Shorter helper macros for more brevity
(defmacro miht ((&rest init-form) &body keys/values)
  `(make-initialized-hash-table (,@init-form) ,@keys/values))

;; Really short macros for common cases.
(defmacro mihteq (&body keys/values)
  `(make-initialized-hash-table (:test #'eq) ,@keys/values))

(defmacro mihteql (&body keys/values)
  `(make-initialized-hash-table (:test #'eql) ,@keys/values))

(defmacro mihtequal (&body keys/values)
  `(make-initialized-hash-table (:test #'equal) ,@keys/values))

(defmacro mihtequalp (&body keys/values)
  `(make-initialized-hash-table (:test #'equalp) ,@keys/values))

;; Since I'm using IOLib, this is portable between implementations
;; upon which IOLib functions. machine-instance turns out to be
;; implemented oddly on different implementations.
(defun hostname ()
  (nth-value 1 (isys:uname)))

;;;;;;;;;;;;;;;;;;;;;;;;;
;; define all of my globals here...
;;;;;;;;;;;;;;;;;;;;;;;;;

;; Should I emit debugging output
(defvar *debug-output* t)

;; The stream which will be the debugging output
(defvar *debug-stream* nil)

;; The configuration table
(defvar *conftable*)

;; The master state
(defvar *mwm*)

;; The stable of slaves
(defvar *stable*)

;; The jar of tasks
(defvar *taskjar*)

;; The slave state
(defvar *mws*)

;; The function which represent the master algorithm
(defvar *master-function*
  ;; We supply a default to let the clmw application writer know they need to
  ;; actually define a master algorithm.
  #'(lambda (argv)
      (alog t
            "MASTER"
            "No master algorithm found. You must define a master algorithm with DEFINE-MW-MASTER.~%")
      255))

;; The function which represents the slave algorithm
(defvar *slave-function*
  ;; The default function is very simple and most likely good enough....
  #'(lambda (argv)
      (alog t "SLAVE" "Using default slave algorithm.~%")
      (mw-slave-loop-simple)))

;; If I have any open connections, store them here. used for cleanup when
;; master dies unexpectedly or other edge cases.
(defvar *open-connections*)

;; The IOLib multiplexer.
(defvar *event-base*)

;; When we dump the executable, this will be bound a list of strings
;; representing the libraries we need to run the executable. The initial
;; binding will be of "./libxxx.so".
;;
;; After the executable starts up, this will be rebound to hold the fully
;; qualified paths to the same libraries.
(defvar *library-dependencies* nil)
