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
    (uiop:quit
     (if (and (integerp ret)
              (>= ret 0)
              (<= ret 255))
         ret
         (progn
           (format *error-output*
                   "MW-INIT: Wrong exit code type/value [~S] in _INIT, assuming 255.~%"
                   ret)
           255)))))

;; Perform the body, which is assumed to open the stream strm in question
;; and write to it.
(defmacro stream->string-list ((strm) &body body)
  (let ((g (gensym))
        (h (gensym)))
    `(let ((,h '()))
       (with-input-from-string
           (,g (with-output-to-string (,strm)
                 (progn
                   ,@body)))
         (loop while (let ((line (read-line ,g nil)))
                       (when line
                         (push line ,h))
                       line))
         (nreverse ,h)))))

(defun canonicalize-flags (flags)
  (destructuring-bind (lib-type &rest others) flags
    (cond
      ((member lib-type '("libc4" "libc5" "libc6") :test #'string-equal)
       (cond
         ;; Handle: ("libc6")
         ((null (car others))
          (list lib-type "x86"))
         ;; Handle: ("libc6" "x86-64"/"x32" ...)
         ((member (car others) '("x32" "x86-64") :test #'string-equal)
          (list* lib-type (copy-seq others)))
         ;; Handle: ("libc6" "OS ABI: Linux x.y.z" ...)
         (t
          (list* lib-type "x86" (copy-seq others)))))
      ((string-equal lib-type "ELF")
       (list "ELF" "" ""))
      (t
       (error "Don't understand lib type/arch flags! Please port!")))))


;; The result of this function is a hash table whose keys are bare-lib names
;; and values are ((flag-list) absolute-file-path) entries in discovery order
;; from ldconfig -p.
(defun parse-ld.so.cache (&key (program "/sbin/ldconfig") (args '("-p")))
  ;; Read all of the output of the program as lines.
  (let ((ht (make-hash-table :test #'equal))
        (lines (stream->string-list
                   (out-stream)
                 (sb-ext:process-close
                  (sb-ext:run-program program args :output out-stream)))))

    ;; Discard first line, it is a count of libs and other junk
    (pop lines)

    ;; Assemble the master hash table which condenses the ldconfig -p info
    ;; into a meaninful object upon which I can query.
    (dolist (line lines)
      (register-groups-bind
          (bare-lib flags absolute-lib)
          ("\\s*(.*)\\s+\\((.*)\\)\\s+=>\\s+(.*)\\s*" line)

        (let ((split-flags
               (canonicalize-flags
                (mapcar #'(lambda (str)
                            (setf str (regex-replace "^\\s+" str ""))
                            (string-trim " " (regex-replace "\\s+$" str "")))
                        (split "," flags)))))

          ;; Insert the lib info into the database keyed by bare-lib.
          (push (list split-flags absolute-lib) (gethash bare-lib ht)))))

    ;; Now that the DB has been created, reverse all of the value
    ;; lists so that the car of the list is the first thing found in
    ;; the output of ldconfig -p.
    (loop for key being the hash-keys in ht using (hash-value val) do
         (setf (gethash key ht) (nreverse val)))
    ht))

;; Convert a bare-lib into a absolute path taking things into consideration
;; such as machine-type.
(defun query-ld.so.cache (bare-lib the-machine-type ht)
  (flet ((find-lib (the-machine-type the-list)
           (find the-machine-type the-list
                 :test (lambda (k v)
                         (member k v
                                 :test #'string-equal))
                 :key #'car)))

    (let ((vht (gethash bare-lib ht)))
      (when vht
        ;; Find the _first_ library with the specified machine-type
        ;; TODO: FIx this to fallback onto an ELF version if it exists and
        ;; I can't find anything else? See libaio.so.1 for example.
        (let ((lib-spec (find-lib the-machine-type  vht)))
          (cadr lib-spec))))))


(defun mw-dump-exec (&key (exec-name "./a.out") ignore-libs remap-libs)
  ;; XXX Does this actually do anything to the saved lisp image? Do
  ;; permutation testing to figure it out.
  (push (truename #P"./") cffi:*foreign-library-directories*)

  (terpri)
  (format t "######################################~%")
  (format t "# Processing loaded shared libraries #~%")
  (format t "######################################~%")
  (let ((shlibs nil)
        (ld.so.cache (parse-ld.so.cache)))
    (dotimes (i (length sb-sys:*shared-objects*))
      (with-slots (pathname namestring)
          (nth i sb-sys:*shared-objects*)

        ;; Any shared library using a fully qualified path gets copied
        ;; to the current working directory (where the executable is
        ;; going to show up) and the in memory shared object
        ;; structures get fixated to know to look in ./ for those
        ;; specific ones.
        ;;
        ;; Any library which is a (assumed) bare library, we look it
        ;; up in the ld.so.cache to find which library we should dump
        ;; to the current working directory. This is assumed to be the
        ;; library that dlopen() would have chosen.
        ;;
        ;; Libraries can be ignored or remapped as desired and ignoring
        ;; trumps remapping.
        (format t "Shared-library: ~A..." namestring)
        (let* ((base (file-namestring namestring))
               (new-path (concatenate 'string "./" base))
               (remap (assoc base remap-libs :test #'equal)))

          (if (member base ignore-libs :test #'equal)
              (format t "ignoring as requested.~%")
              (progn
                (cond
                  ;; If the library has a remap, let's honor it.
                  (remap
                   (let* ((from (cadr remap))
                          (to (concatenate 'string "./"
                                           (file-namestring from))))
                     (unless (equal from to)
                       (format t "dumping remapped library ~A..." from)
                       (copy-a-file from to))
                     ;; Fix up the new-path for the later fixating phase.
                     (setf new-path to)))

                  ;; If the library is already an absolute path, copy it over
                  ((char-equal (char namestring 0) #\/)
                   (format t "dumping...")
                   (copy-a-file namestring new-path))

                  ;; Otherwise approximate the dlopen algorithm and
                  ;; convert the bare (hopefully) library to an
                  ;; absolute path.
                  (t
                   (format t "looking up for machine-type ~S..." (machine-type))
                   (let ((abs-lib
                          (query-ld.so.cache namestring (machine-type)
                                             ld.so.cache)))
                     (format t "found ~A..." abs-lib)
                     (format t "dumping...")
                     (copy-a-file abs-lib new-path))))

                (format t "fixating.~%")
                ;; Reset the in memory shared library object to
                ;; reference the local one right here. This means
                ;; wherever you run the executable, the shared
                ;; libraries better be in the same directory as the
                ;; executable.
                (setf namestring new-path)
                (setf namestring
                      (sb-alien::native-namestring
                       (translate-logical-pathname new-path) :as-file t))
                (push new-path shlibs))))))

    ;; Done processing the libraries...
    (unless (null shlibs)
      ;; Store the relative libraries for later chicanery when we restart
      (setf *library-dependencies* shlibs)
      (terpri)
      (format t "########################################################~%")
      (format t "#  Please package these libraries with your executable #~%")
      (format t "########################################################~%")
      (format t "~{~A~%~}" shlibs)))

  (terpri)
  (format t "####################################~%")
  (format t "# Writing Master/Slave executable #~%")
  (format t "####################################~%")
  (sb-ext:save-lisp-and-die exec-name :toplevel #'_init :executable t
                            :purify t :save-runtime-options t))
