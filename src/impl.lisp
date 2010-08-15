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

;; SBCL specific
(defun _init ()
  ;; iolib uses shared libraries, so amend the search path to find them.
  (push (truename #P"./") cffi:*foreign-library-directories*)

  ;; Ok, using the truename of the executable location in the first position of
  ;; argv, we can rip off the initial path, and use it to locate the rest of
  ;; the libraries that should be next to it.
  ;;
  ;; XXX This whole process of dealing with the required shared
  ;; libraries is a bit wonky.....
  (let ((dir (namestring
              (truename
               (make-pathname :directory
                              (pathname-directory
                               (car sb-ext::*posix-argv*)))))))
    (setf *library-dependencies*
          (mapcar #'(lambda (p)
                      (namestring
                       (truename
                        (pathname
                         (concatenate 'string dir p)))))
                  *library-dependencies*)))

  ;; WARNING: The system argv is handled in the mw-initialize keyword
  ;; arguments! Handling it there allows one to call mw-initialize in
  ;; the REPL and still have a meaningful argv[0].
  (let ((ret (mw-initialize '())))
    (finish-output)
    ;; Here we do a "belt and suspenders" check to ensure we return something
    ;; that is always valid.
    (sb-ext:quit :unix-status
                 (if (and (integerp ret)
                          (>= ret 0)
                          (<= ret 255))
                     ret
                     (progn
                       (format *error-output*
                               "MW-INIT: Wrong exit code type/value [%S] in _INIT, assuming 255.~%"
                               ret)
                       255)))))

;; SBCL specific
;; :all, :automatic, :none
(defun mw-dump-exec (&key (exec-name "./a.out") (libs :automatic))
  ;; XXX Does this actually do anything to the saved lisp image? Do
  ;; permutation testing to figure it out.
  (push (truename #P"./") cffi:*foreign-library-directories*)

  (terpri)
  (format t "######################################~%")
  (format t "# Processing loaded shared libraries #~%")
  (format t "######################################~%")
  (let ((shlibs nil))
    (unless (eq libs :none)
      (dotimes (i (length sb-sys:*shared-objects*))
        (with-slots (pathname namestring)
            (nth i sb-sys:*shared-objects*)

          ;; Any shared library using a fully qualified path gets copied
          ;; to the current working directory (where the executable is
          ;; going to show up) and the in memory shared object
          ;; structures get fixated to know to look in ./ for those
          ;; specific ones.
          (format t "Shared-library: ~A..." namestring)
          (if (char-equal (char namestring 0) #\/)
              (progn
                (let ((base (concatenate 'string "./"
                                         (file-namestring namestring))))
                  (format t "dumping...")
                  ;; Copy the library from wherever it was originally
                  ;; found to here.
                  (copy-file namestring base)

                  (format t "fixating.~%")
                  ;; Reset the in memory shared library object to
                  ;; reference the local one right here. This means
                  ;; wherever you run the executable, the shared
                  ;; libraries better be in the same directory as the
                  ;; executable.
                  (setf namestring base)
                  (setf pathname (pathname base))
                  (push base shlibs)))
              (format t "ignore.~%"))))

      (unless (null shlibs)
        ;; Store the relative libraries for later understanding when we restart
        (setf *library-dependencies* shlibs)
        (terpri)
        (format t "########################################################~%")
        (format t "#  Please package these libraries with your executable #~%")
        (format t "########################################################~%")
        (format t "~{~A~%~}" shlibs))))

  (terpri)
  (format t "####################################~%")
  (format t "# Writing Master/Slave executable #~%")
  (format t "####################################~%")
  (sb-ext:save-lisp-and-die exec-name :toplevel #'_init :executable t
                            :purify t :save-runtime-options t))


;; Efficiently copy a file from one location on disk to another
;; location on disk.
(defun copy-file (iname oname &optional (buffer-size (* 1024 1024)))
  (with-open-file (fin iname :direction :input
                       :element-type '(unsigned-byte 8))
    (with-open-file (fout oname :direction :output
                          :element-type '(unsigned-byte 8)
                          :if-exists :supersede
                          :if-does-not-exist :create)

      (do* ((buffer (make-array buffer-size :element-type
                                (stream-element-type fin)))
            (nread (read-sequence buffer fin) (read-sequence buffer fin))
            (total 0 (+ total nread)))
           ((zerop nread) total)
        (write-sequence buffer fout :end nread)))))
