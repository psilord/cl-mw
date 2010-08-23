(defpackage #:cl-mw-asd
  (:use #:cl #:asdf))
(in-package #:cl-mw-asd)

(defsystem #:cl-mw
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
                                            :depends-on ("mw"))))

               ;; The hello-world example application
               (:module module-example-hello-world
                        :depends-on (module-cl-mw)
                        :pathname "examples/hello-world"
                        :components ((:file "package")
                                     (:file "hello-world"
                                            :depends-on ("package"))))

               ;; The ping example application
               (:module module-example-ping
                        :depends-on (module-cl-mw)
                        :pathname "examples/ping"
                        :components ((:file "package")
                                     (:file "ping"
                                            :depends-on ("package"))))

               ;; The monte-carlo-pi example application
               (:module module-example-monte-carlo-pi
                        :depends-on (module-cl-mw)
                        :pathname "examples/monte-carlo-pi"
                        :components ((:file "package")
                                     (:file "monte-carlo-pi"
                                            :depends-on ("package"))))

               ;; The higher-order example application
               (:module module-example-higher-order
                        :depends-on (module-cl-mw)
                        :pathname "examples/higher-order"
                        :components ((:file "package")
                                     (:file "higher-order"
                                            :depends-on ("package"))))))
