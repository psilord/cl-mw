(defpackage #:cl-mw-examples-monte-carlo-pi-asd
  (:use #:cl #:asdf))

(in-package #:cl-mw-examples-monte-carlo-pi-asd)

(defsystem #:cl-mw.examples.monte-carlo-pi
  :depends-on (#:cl-mw)
  :components (
               ;; The monte-carlo-pi example application
               (:module module-example-monte-carlo-pi
                        :pathname "examples/monte-carlo-pi"
                        :components ((:file "package")
                                     (:file "monte-carlo-pi"
                                            :depends-on ("package"))))))

