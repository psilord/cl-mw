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

(in-package :cl-mw)

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
