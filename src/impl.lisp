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

;; Convert the machine type to a keyword.
(defun get-machine-type ()
  (let ((mt (machine-type)))
    (cond
      ((equalp "X86" mt)
       :x86)
      ((equalp "X86-64" mt)
       :x86-64)
      (t
       (error "Unknown machine type ~A, please port!~%" mt)))))

;; Given a list of flags associated with a line from ldconfig -p, find me
;; the library type the library is.
(defun find-lib-type (split-flags)
  (if (find "libc6" split-flags :test #'equalp)
      "libc6"
      (if (find "ELF" split-flags :test #'equalp)
          "ELF"
          (assert "Unknown lib type. Please port!~%"))))

;; Given a list of flags associated with a line from the ldconfig -p, find
;; me the specific architecture associated with the library.
(defun find-lib-arch (split-flags)
  (if (find "x86-64" split-flags :test #'equalp)
      :x86-64
      :x86))

;; Take a line from the ldconfig -p output and merge it with the rest
;; of the lines in the hashtable ht.
(defun merge-ld.so.cache-line (bare-lib split-flags absolute-lib ht)
  ;; Ensure the bare-lib has a hash table entry in the master table.
  (when (null (gethash bare-lib ht))
    (setf (gethash bare-lib ht) (make-hash-table :test #'equal)))

  ;; The type of the library is either libc6 or ELF, but not both. So
  ;; find out which one it is and set the type in the hash table value
  ;; for the library in question. Ensure the type didn't change!
  (let ((lib-type (find-lib-type split-flags))
        (vht (gethash bare-lib ht)))
    (let ((prev-lib-type (gethash :type vht)))
      (if (null prev-lib-type)
          (setf (gethash :type vht) lib-type)
          (when (not (equal prev-lib-type lib-type))
            (error "~A changed library type!" bare-lib)))))

  ;; For each arch, if the value list doesn't exist, make one and
  ;; insert it, otherwise insert the entry at the end of the list. We
  ;; do it at the end because we're following the search order as
  ;; found in order.
  (let ((lib-arch (find-lib-arch split-flags))
        (vht (gethash bare-lib ht)))
    (let ((prev-lib-list (gethash lib-arch vht)))
      (if (null prev-lib-list)
          (setf (gethash lib-arch vht) (list absolute-lib))
          (rplacd (last (gethash lib-arch vht)) (list absolute-lib))))))

;; The result of this function is a hash table which contains entries
;; about how to map bare library names to absolute paths as generated
;; from ldconfig -p. If a library maps to more than one library in the same
;; architecture, they are preserved in the order of discovery from left to
;; right in the list.
;;
;; In a perl-ish dialect, you get:
;; %hash = (
;;   "libm.so.6" => (
;;      :type => "libc6"
;;      :X86-64 => ("/lib64/libm.so.6")
;;      :X86 => ("/lib/tls/libm.so.6" "/lib/i686/libm.so.6" "/lib/libm.so.6")
;;   )
;;   "libGLU.so.1" => (
;;      :type => "libc6"
;;      :X86-64 => ("/usr/X11R6/lib64/libGLU.so.1" "/usr/lib64/libGLU.so.1")
;;      :X86 => ("/usr/X11R6/lib/libGLU.so.1" "/usr/lib/libGLU.so.1")
;;   )
;; )
(defun parse-ld.so.cache (&key (program "/sbin/ldconfig") (args '("-p")))
  ;; Read all of the output of the program as lines.
  (let ((ht (make-hash-table :test #'equal))
        (lines (stream->string-list
                (out-stream)
                (sb-ext:process-close
                 (sb-ext:run-program program args :output out-stream)))))

    ;; Pop the first line off, it is a count of libs and other junk
    (pop lines)

    ;; Assemble the master hash table which condenses the ldconfig -p info
    ;; into a meaninful object upon which I can query.
    (dolist (line lines)
      (register-groups-bind
       (bare-lib flags absolute-lib)
       ("\\s*(.*)\\s+\\((.*)\\)\\s+=>\\s+(.*)\\s*" line)

       (let ((split-flags
              (mapcar #'(lambda (str)
                          (setf str (regex-replace "^\\s+" str ""))
                          (regex-replace "\\s+$" str ""))
                      (split "," flags))))
         (merge-ld.so.cache-line bare-lib split-flags absolute-lib ht))))
    ht))

;; Convert a bare-lib into an absolute path depending upon
;; architecture and whatnot. Either returns an absolute path, or nil.
;;
;; Can specify an ordering of :all, :first (the default), or :last.
;; The ordering of :all will present the lirbaries in the order found
;; out of the output for ldconfig -p.
(defun query-ld.so.cache (bare-lib ht &key (ordering :first))
  (let ((vht (gethash bare-lib ht)))
    (if (null vht)
        nil
        (let ((all-absolute-libs (gethash (get-machine-type) vht)))
          (ecase ordering
            (:all
             all-absolute-libs)
            (:first
             (car all-absolute-libs))
            (:last
             (last all-absolute-libs)))))))


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
                   (format t "looking up...")
                   (let ((abs-lib (query-ld.so.cache namestring ld.so.cache)))
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
