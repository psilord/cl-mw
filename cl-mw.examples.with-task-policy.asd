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

(defpackage #:cl-mw-examples-with-task-policy-asd
  (:use #:cl #:asdf))

(in-package #:cl-mw-examples-with-task-policy-asd)

(defsystem #:cl-mw.examples.with-task-policy
  :depends-on (#:cl-mw)
  :components (
               ;; The with-task-policy example application
               (:module module-example-with-task-policy
                        :pathname "examples/with-task-policy"
                        :components ((:file "package")
                                     (:file "with-task-policy"
                                            :depends-on ("package"))))))

