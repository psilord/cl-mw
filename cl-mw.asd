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

(defpackage #:cl-mw-asd
  (:use :cl :asdf))
(in-package #:cl-mw-asd)

(defsystem #:cl-mw
  :description "CL-MW: A Master/Slave Library"
  :version "0.2"
  :author "Peter Keller <psilord@cs.wisc.edu>"
  :licence "Apache License, Version 2.0"

  :depends-on (#:hu.dwim.serializer #:alexandria #:iolib #:cffi #:cl-ppcre)
  :components (
               ;; This is the CL-MW library source code
               (:module module-cl-mw
                        :pathname "src"
                        :components ((:file "package")
                                     (:file "structures"
                                            :depends-on ("package"))
                                     (:file "packet-buffer"
                                            :depends-on ("package"))
                                     (:file "stable"
                                            :depends-on ("structures"))
                                     (:file "taskjar"
                                            :depends-on ("stable"))
                                     (:file "mw"
                                            :depends-on ("taskjar"
                                                         "stable"
                                                         "structures"
                                                         "packet-buffer"))
                                     (:file "file-utils"
                                            :depends-on ("package"))
                                     (:file "impl"
                                            :depends-on ("mw" "file-utils"))))))
