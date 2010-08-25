(defpackage #:cl-mw-examples-higher-order-asd
  (:use #:cl #:asdf))

(in-package #:cl-mw-examples-higher-order-asd)

(defsystem #:cl-mw.examples.higher-order
  :depends-on (#:cl-mw)
  :components (
               ;; The higher-order example application
               (:module module-example-higher-order
                        :pathname "examples/higher-order"
                        :components ((:file "package")
                                     (:file "higher-order"
                                            :depends-on ("package"))))))

