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

;;;; The slave stable is a place where we keep track of the slaves and
;;;; what states they are in. It allows us too group slaves by status
;;;; and look them up via who/port combinations or their sids. The
;;;; 'slaves' table holds the actual slave structures, and all the
;;;; other tables are paired by how we need to look up a slave. A
;;;; table like 'idle-slaves' and 'idle-slaves-by-sid' are connected
;;;; in that if a slave reference exsts in one, it'll exist in the
;;;; other. The 'connecting,idle,busy,disconnected' keywords represent
;;;; "stalls" (as in what you keep an animal). A slave MUST be in the
;;;; base 'slaves' hash AND *ONE* other stall (which is two hash
;;;; tables).  The functions in this file keep everything internally
;;;; consistent.

;; Give me a settable reference to the right hash given that I need
;; :who-port or :sid
(defun gss (where &optional which)      ; get-slave-stall
  (ecase where
    (:base
     (mw-stable-slaves *stable*))
    (:kind
     (ecase which
       (:who-port
        (mw-stable-kind *stable*))
       (:sid
        (mw-stable-kind-by-sid *stable*))))
    (:connecting
     (ecase which
       (:who-port
        (mw-stable-connecting-slaves *stable*))
       (:sid
        (mw-stable-connecting-slaves-by-sid *stable*))))
    (:idle
     (ecase which
       (:who-port
        (mw-stable-idle-slaves *stable*))
       (:sid
        (mw-stable-idle-slaves-by-sid *stable*))))
    (:busy
     (ecase which
       (:who-port
        (mw-stable-busy-slaves *stable*))
       (:sid
        (mw-stable-busy-slaves-by-sid *stable*))))
    (:shutting-down
     (ecase which
       (:who-port
        (mw-stable-shutting-down-slaves *stable*))
       (:sid
        (mw-stable-shutting-down-slaves-by-sid *stable*))))
    (:disconnected
     (ecase which
       (:who-port
        (mw-stable-disconnected-slaves *stable*))
       (:sid
        (mw-stable-disconnected-slaves-by-sid *stable*))))))

;; Add a slave to the stable, putting it into a view where appropriate
;; This modifies the status of the slave to whatever stall it is going.
;; It also adds the slave to the slave kind tables using the information
;; in the slave object as to where it goes.
(defun add-slave (slave who port sid &key (where :connecting))
  (with-slots (sid status ordered last-heard-from) slave
    ;; Tell the slave in what stall it lives and when I put it there.
    (setf status where
          last-heard-from (get-universal-time))

    (let ((key (format nil "~A:~A" who port)))

      ;; Add the slave to the basic slave table
      (skvh key slave (gss :base))
      ;; Put it into the right stall by who/port
      (skvh key key (gss where :who-port))
      ;; Put it into the right stall by sid
      (skvh sid key (gss where :sid))

      ;; Add the slave to the "slave kind" tables
      (skvh key key (lkh ordered (gss :kind :who-port)))
      (skvh sid key (lkh ordered (gss :kind :sid))))))

;; Return as two values the who/port of a slave and the stall in which the
;; slave exists. nil nil of no slave present by the key provided. A key thing
;; to realize here is that a slave may ONLY exist in one stall.
(defun find-slave-helper (&key who port sid)
  ;; I can have either a who port combination or a sid, but not both or none
  (assert (and (or (and who port)
                   sid)
               (not (and who port sid))))

  (let* ((what (if sid sid (format nil "~A:~A" who port)))
         (where (if sid :sid :who-port))

         ;; Lookup the 'what' in each 'where'
         (a (multiple-value-list
             (lkh what (gss :connecting where))))
         (b (multiple-value-list
             (lkh what (gss :idle where))))
         (c (multiple-value-list
             (lkh what (gss :busy where))))
         (d (multiple-value-list
             (lkh what (gss :shutting-down where))))
         (e (multiple-value-list
             (lkh what (gss :disconnected where)))))
    ;; For the view which contained it, return it.
    (cond
      ((cadr a)
       (values (car a) :connecting))
      ((cadr b)
       (values (car b) :idle))
      ((cadr c)
       (values (car c) :busy))
      ((cadr d)
       (values (car d) :shutting-down))
      ((cadr e)
       (values (car e) :disconnected))
      (t
       (values nil nil)))))

;; Like find-slave-helper, but this returns the actual slave structure
;; as well as what stall it exists as multiple values.
(defun find-slave (&key who port sid)
  ;; I can have either a who port combination or a sid, but not both or none
  (assert (and (or (and who port)
                   sid)
               (not (and who port sid))))

  (multiple-value-bind (who-port where)
      (if sid
          (find-slave-helper :sid sid)
          (find-slave-helper :who who :port port))
    (values (lkh who-port (gss :base)) where)))

;; Remove the slave out of the stable, and return two values, the
;; slave structure and the stall in which it was found. We do NOT
;; affect the status of the slave, since it may have been removed from
;; the stable for a reason but is still active. While we remove the
;; slave from the kind tables too, we don't pass back in what kind
;; group the slave had been in since you can just inspect the
;; ordered file in the slave object to see.
(defun remove-slave (&key who port sid)
  ;; I can have either a who port combination or a sid, but not both or none
  (assert (and (or (and who port)
                   sid)
               (not (and who port sid))))

  (if sid
      ;; Look up by sid first, then get it from the base
      (multiple-value-bind (who-port where)
          (find-slave-helper :sid sid)
        (if who-port
            (progn
              ;; Remove it from the two views
              (rkh sid (gss where :sid))
              (rkh who-port (gss where :who-port))
              ;; Get the actual slave from the slaves, remove it,
              ;; return the values
              (let ((slave (lkh who-port (gss :base))))
                ;; Remove it from the kind tables
                (rkh sid
                     (lkh (mw-slave-ordered slave) (gss :kind :sid)))
                (rkh who-port
                     (lkh (mw-slave-ordered slave) (gss :kind :who-port)))
                ;; Finally remove it from the base
                (rkh who-port (gss :base))
                (values slave where)))
            (values nil nil)))

      ;; Look up in base first, ask for sid from slave, then do the
      ;; other views.
      (multiple-value-bind (who-port where)
          (find-slave-helper :who who :port port)
        (if who-port
            (progn
              (let* ((slave (lkh who-port (gss :base)))
                     (sid (mw-slave-sid slave)))
                ;; Remove it from the two views
                (rkh sid (gss where :sid))
                (rkh who-port (gss where :who-port))
                ;; Remove it from the kind tables
                (rkh sid
                     (lkh (mw-slave-ordered slave) (gss :kind :sid)))
                (rkh who-port
                     (lkh (mw-slave-ordered slave) (gss :kind :who-port)))
                ;; Remove it from the base and return it
                (rkh who-port (gss :base))
                (values slave where)))
            (values nil nil)))))

;; Remove slaves from the stalls requested, or the whole stable if no
;; stalls specified. Returns a list of pairs where the car is the
;; slave structure and the cdr is in what stall it used to live.
(defun remove-slaves (&key (from '(:connecting :idle :busy :shutting-down
                                   :disconnected)))
  ;; Canonicalize from into a list with a default
  (let ((from (if (listp from) from (list from)))
        (results nil))

    ;; This is split into two pieces because the complexity of it
    ;; makes me fear removing from hash tables I'm iterating
    ;; across. The first piece collects the information, the next
    ;; piece removes them.

    ;; 1. For each stall requested, accumulate a list of the sids
    (dolist (where from)
      (maphash #'(lambda (k v)
                   (push (list k where) results))
               (gss where :sid)))

    ;; 2. Now remove all of the sids found, returning a list of pairs
    ;; of slave structure associated with the stall they were in.
    (mapcar #'(lambda (pair)
                (multiple-value-list (remove-slave :sid (car pair))))
            results)))

;; Find and return the first slave in the from specified, looking in
;; order.  Return a values which is the slave, and where it was
;; found. If no slave can be found, nil nil is returned. Defaults to
;; looking for idle slaves only.
(defun find-a-slave (&key (from '(:idle)))
  ;; Canonicalize from
  (let ((from (if (listp from) from (list from))))
    ;; Lookup the first findable one in the stall
    (dolist (where from)
      (let ((keys (hash-table-keys (gss where :sid))))
        (when keys
          (return (find-slave :sid (car keys))))))))

;; return a list of all slaves from a set of stalls, default just to idle
(defun find-all-slaves (&key (from '(:idle)))
  (let ((from (if (listp from) from (list from)))
        (result nil))

    (dolist (where from)
      ;; get a list of all keys (which are sids) from the specific stall
      (let ((keys (hash-table-keys (gss where :sid))))
        ;; gather the actual slaves into a list
        (mapc #'(lambda (k)
                  (push (find-slave :sid k) result))
              keys)))
    result))


;; This function modifies the slave structure by adjusting the status
;; field when it moves it from one spot to another.
(defun move-slave (&key who port sid where)
  ;; I can have either a who port combination or a sid, but not both or none
  (assert (and (or (and who port)
                   sid)
               (not (and who port sid))))
  (assert where)

  (let ((free-slave (if sid
                        (remove-slave :sid sid)
                        (remove-slave :who who :port port))))
    (if free-slave
        ;; add-slave needs a fully resolved who and port binding, so here
        ;; we figure out how we were called and ensure there is always a valid
        ;; who and port.
        (let ((who (if sid (mw-slave-who free-slave) who))
              (port (if sid (mw-slave-port free-slave) port)))
          (add-slave free-slave who port (mw-slave-sid free-slave)
                     :where where))
        nil)))


;; If the target state is not specified, it is :connecting. This modifies the
;; status field to be where you put it.
;;(add-slave slave who port sid :where :connecting)

;; Using either who/port or sid, the slave is found in whatever stall it which
;; it resides and is moved to where the user wanted.
;;(move-slave :who who :port port :where :disconnected)
;;(move-slave :sid sid :where :disconnected)

;; Remove slave out of the stable and return it. First value is the
;; slave or nil if not present, second value is the state the slave
;; had been in when it was removed or nil if not present
;;(remove-slave :who who :port port)
;;#<slave>
;;state

;;(remove-slave :sid sid)
;;#<slave>
;;state

;; in this form, you get a list back as the first return form. The form
;; can be a list, in which case the union is performed.
;;(remove-slaves :from :disconnected)
;;[list of (slave state) pairs]

;; In this form, all slaves are removed and a list of the slave and
;; the state it was in was returned
;;(remove-slaves)
;;[list of (slave state) pairs]

;; returns two values, the specified slave structure or nil if not
;; present, and the state it is in or nil if no slave
;;(find-slave :sid sid)
;;#<slave>
;;:idle

;;(find-slave :who who :port port)
;;#<slave>
;;:idle

;; finds the first slave which succeeds in meeting the requirements. Return
;; nil if no such slave or the slave structure
;;(find-a-slave :from :idle)
;;#<slave>

;;(find-a-slave :who who :port port :from :idle)
;;#<slave>

;;(find-a-slave :sid sid :from :idle)
;;#<slave>

;; Move the slaves from the first state to the second state after passing the
;; slave through the supplied function.
                                        ;(map-slaves #'assign-tasks-to-slave
;;            :from :idle :to :busy)

;; map a known slave from one state to another through the function
;;(map-slaves #'assign-tasks-to-slave
;;            :who who :port port
;;            :from :idle :to :busy)

;;(map-slaves #'assign-tasks-to-slave
;;            :sid sid
;;            :from :idle :to :busy)

;; run the function on all slaves in the requested state
;;(map-slaves #'reassign-tasks :in :disconnected)


;;(defun assign-tasks-to-slave (slave (&key who port sid))
;;  (let ((tasks (dequeue (mw-state-runnable-tasks *mws*)
;;                        (mw-slave-group slave))))
;;    (setf (mw-slave-pending-tasks slave) tasks)
;;    (incf (me-slave-num-pending-tasks slave) (mw-slave-group slave))



