(defpackage #:cl-mw-asd
  (:use #:cl #:asdf))
(in-package #:cl-mw-asd)

(defsystem #:cl-mw
  :description "CL-MW: A Master/Worker Library"
  :version "0.1"
  :author "Peter Keller <psilord@cs.wisc.edu>"
  :licence "Apache License, Version 2.0"
  
  :depends-on (#:hu.dwim.serializer #:alexandria #:iolib)
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
                                     (:file "impl"
                                            :depends-on ("mw"))))))
