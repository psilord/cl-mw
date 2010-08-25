(defpackage #:cl-mw-examples-hello-world-asd
  (:use #:cl #:asdf))

(in-package #:cl-mw-examples-hello-world-asd)

(defsystem #:cl-mw.examples.hello-world
  :depends-on (#:cl-mw)
  :components (
               ;; The hello-world example application
               (:module module-example-hello-world
                        :pathname "examples/hello-world"
                        :components ((:file "package")
                                     (:file "hello-world"
                                            :depends-on ("package"))))))

