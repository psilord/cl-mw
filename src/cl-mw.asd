
(asdf:defsystem #:cl-mw
  :depends-on (#:hu.dwim.serializer
	       #:alexandria
	       #:iolib)
  :components ( (:file "package")

                (:file "structures"
                       :depends-on ("package"))

                (:file "packet-buffer"
                       :depends-on ("package"))

                (:file "stable"
                       :depends-on ("structures"))

                (:file "taskjar"
                       :depends-on ("stable"))

                (:file "mw"
                       :depends-on ("taskjar" "stable" "structures"
					      "packet-buffer"))

		(:file "impl"
		       :depends-on ("mw"))))

