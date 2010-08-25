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

;; Exported to the user these are tasks that can be run or are running.
(defun mw-num-runnable-tasks ()
  (hash-table-count (mw-taskjar-tasks *taskjar*)))

;; Exported to the user.
(defun mw-num-unrunnable-tasks ()
  (length (mw-taskjar-unrunnable-tasks *taskjar*)))

;; Exported to user.
(defun mw-get-unrunnable-tasks ()
  (let ((res (mw-taskjar-unrunnable-tasks *taskjar*)))
    (setf (mw-taskjar-unrunnable-tasks *taskjar*) nil)
    ;; Walk the task list and convert the serialized data back to unserialized
    ;; so the master algorithm can inspect the payload easily.
    (dolist (r res)
      (setf (mw-task-packet r) (deserialize (mw-task-packet r))))
    res))

;; This ensures there is a empty queue in the ht at the key before enqueing
;; the value into it. Of course, if the value already exists, then just
;; enqueue it. We use this for various queues which are hashtable values.
(defun safe-enqueue-hash (key value ht)
  (multiple-value-bind (v p) (gethash key ht)
    (unless p
      (setf (gethash key ht) (make-queue))))
  (enqueue value (gethash key ht)))

;; Push up or bring down, depending upon the inc/dec function being
;; either #'1+ or #'1- respectively, the general target-number level
;; and the individual one associated with the task.
(defun adjust-pending-target-numbers (inc/dec task)
  (with-slots (target-numbers tasks) *taskjar*
    (with-slots (algorithm-pending-tasks general-pending) target-numbers
      ;; Increment or decrement the general pending target number
      (setf general-pending (mw-zero-clamp (funcall inc/dec general-pending)))
      ;; Increment of decrement the specific algorithm pending target
      ;; number
      (with-slots (algorithm) task
        (multiple-value-bind (level present)
            (lkh algorithm algorithm-pending-tasks)
          (skvh algorithm
                (mw-zero-clamp (funcall inc/dec (if present level 0)))
                algorithm-pending-tasks))))))

;; Put a task into the tasks hash in *taskjar* and update any
;; target-number pending counts that go along with adding it.
(defun store-base-task (id task)
  (with-slots (target-numbers tasks) *taskjar*
    ;; store it in the  base task table
    (skvh id task tasks))
  (adjust-pending-target-numbers #'1+ task))

;; Adds a brand new task structure to the taskjar or possibly the
;; unrunnable queue under certain conditions.  Returns t if the task
;; was added somewhere, nil if the task went immediately to the
;; unrunnable test.
(defun add-task (task)
  (with-slots (id sid do-it-anyway) task
    (let ((slave (when sid (find-slave :sid sid))))

      (if (not sid)
          ;; Task does not desire to be placed to a slave, so it is an
          ;; unordered task.

          (progn
            (emit nil "Adding an unordered task: ~S~%" id)
            ;; Store the actual task structure
            (store-base-task id task)
            ;; Store a reference to the task in the unordered set
            (skvh id id (mw-taskjar-unordered-tasks *taskjar*))
            t)

          ;; Task is ordered since it is meant for a slave. See if
          ;; the slave exists, is willing to take the task, and what
          ;; happens if it doesn't.

          (if (and slave (ordered-slavep slave))
              ;; If the slave exists, good, we'll add the taskid to
              ;; the ordered-tasks queue for that slave and it'll
              ;; run eventually.
              (progn
                (emit nil "Adding a ordered task ~s~%" task)
                ;; Store the actual task structure
                (store-base-task id task)
                ;; Add a reference to the task in the ordered table
                (safe-enqueue-hash sid id
                                   (mw-taskjar-ordered-tasks *taskjar*))
                t)

              ;; There is a sid, but no slave instance (because
              ;; presumeably the slave went away and we removed it
              ;; from the slave table) or the slave doesn't want to accept
              ;; ordered tasks, what happens?
              (if do-it-anyway
                  ;; If the user wanted to do it anyway, then fix up
                  ;; the task and queue it up for any slave to do.
                  (progn
                    (emit nil "Adding converted unordered dia task ~s~%" task)
                    ;; Get rid of the fact we needed a sid:
                    (setf sid nil)
                    ;; Store the actual task structure
                    (store-base-task id task)
                    ;; Add a reference to the unordered task set
                    (skvh id id (mw-taskjar-unordered-tasks *taskjar*))
                    t)

                  ;; In this case, the user specifically wanted the slave and
                  ;; no other, if the slave isn't present or doesn't want it,
                  ;; the task loses and goes unrunnable.
                  (progn
                    (emit nil "Added task is unrunnable! ~s~%" task)
                    (push task (mw-taskjar-unrunnable-tasks *taskjar*))
                    nil)))))))

;; Find the real task structure, or nil, if it isn't in the taskjar.
(defun find-task (task-id)
  (lkh task-id (mw-taskjar-tasks *taskjar*)))

;; Remove the task totally from the taskjar, but leave any speculations that
;; might be around referencing it. It is resolve-speculation that will only
;; remove the task-id from that table. Return the task, or nil if not found.
(defun remove-task (task-id)
  ;; Remove the references to it from the unordered or ordered sets. One
  ;; of these will be successful.
  (rkh task-id (mw-taskjar-unordered-tasks *taskjar*))
  (rkh task-id (mw-taskjar-ordered-tasks *taskjar*))

  ;; Remove it from the task table itself, but return the task as the result.
  (let ((tsk (lkh task-id (mw-taskjar-tasks *taskjar*))))
    (rkh task-id (mw-taskjar-tasks *taskjar*))
    tsk))

;; Depending upon what the slave wants, partition out some of the
;; tasks and enqueue them into the slave. The thing actually being
;; enqueued are merely the task-ids. However the task-id references
;; are removed from the taskjar sets that contained them.  This
;; function will also add the slave to the speculation structure for
;; each task-id. If we want to be speculating, then we can find tasks
;; we're already speculating upon, and send them off to be run in a
;; duplicate manner. In this case, we add the slave to the speculation
;; structures associated with the tasks we speculate.
;;
;; Returns t if some tasks were allocated, nil otherwise.
(defun allocate-tasks (slave)

  ;; Preference of scheduling concerning field ordered:
  ;; 1. Schedule ordered tasks only on :ordered.
  ;; 2. Schedule ordered first, then unordered if needed, on :intermingle
  ;; 3. Schedule unordered tasks on slave
  ;; 4. If speculation is wanted, allocate some already speculated tasks.

  (with-slots (ordered) slave
    (ecase ordered
      (:ordered
       (allocate-ordered-tasks slave))
      (:intermingle
       (allocate-intermingle-tasks slave))
      (:unordered
       (allocate-unordered-tasks slave)))))

;; Here we try and fill up the pending-queue of the slave as much as possible
;; with only unordered tasks. We adjust the num-pending-tasks number in the
;; slave to be however many were were able to allocate.
(defun allocate-unordered-tasks (slave)
  (with-slots
        (ordered task-group pending-task-queue num-pending-tasks) slave

    ;; Only these types of slaves can accept unordered tasks.
    (assert (or (eq :unordered ordered)
                (eq :intermingle ordered)))

    (let ((orig-num-pending-tasks num-pending-tasks))
      (block all-done
        (with-hash-table-iterator
            (next-entry (mw-taskjar-unordered-tasks *taskjar*))
          (loop
             (multiple-value-bind (more? task-id value) (next-entry)
               (cond
                 ((not more?)
                  (return-from all-done nil))

                 ((< num-pending-tasks task-group)

                  ;; We are now speculating this task on the slave, record it.
                  (add-speculation slave task-id)

                  ;; Add the task to the pending queue
                  (enqueue task-id pending-task-queue)
                  (incf num-pending-tasks)

                  ;; ANSI CL states removal of the current entry is ok.
                  ;; When this task is speculated at all, it is out of the
                  ;; unordered set, so this function couldn't respeculate it,
                  ;; there is a special allocate-speculating-tasks function
                  ;; for that behavior which determines under what conditions
                  ;; speculation may happen.
                  (rkh task-id (mw-taskjar-unordered-tasks *taskjar*))

                  ;; Now, since this task conceptually went form
                  ;; pending to allocated and running, we bookkeep the
                  ;; target-number pending count for the algorithm for
                  ;; which the task is intended.
                  (adjust-pending-target-numbers #'1- (find-task task-id)))

                 (t
                  ;; If I have more entries left, but I've reached my
                  ;; task-group limit, then I'm all finished.
                  (return-from all-done nil)))))))

      ;; If I added some tasks to the slaves queue, then return true.
      (/= num-pending-tasks orig-num-pending-tasks))))


;; Here we try to fill up the pending queue with ordered task destined for
;; this slave. We return t if we allocated some tasks, nil if we didn't.
;; This function adds a speculation for each task allocated as well.
(defun allocate-ordered-tasks (slave)
  ;; If there are no ordered tasks for this slave, we're done.
  (with-slots
        (sid ordered task-group pending-task-queue num-pending-tasks) slave

    (assert (or (eq :ordered ordered) (eq :intermingle ordered)))

    (multiple-value-bind (queue present)
        (lkh sid (mw-taskjar-ordered-tasks *taskjar*))

      ;; Bail if there are no ordered tasks to enqueue up into the slave.
      (unless (and present (not (empty-queue queue)))
        (return-from allocate-ordered-tasks nil))

      ;; If we have any room in the slave's pending queue, we cram some more
      ;; tasks in there.
      (let ((orig-num-pending-tasks num-pending-tasks))
        (while (not (or (empty-queue queue) (= num-pending-tasks task-group)))
          (let ((task-id (dequeue queue)))
            (add-speculation slave task-id)
            (enqueue task-id pending-task-queue)
            (incf num-pending-tasks)

            ;; Now, since this task conceptually went form pending to
            ;; allocated and running, we bookeep the target-number
            ;; count for the algorithm for which the task is intended.
            (adjust-pending-target-numbers #'1- (find-task task-id))))

        ;; Did I queue up any new tasks into the slave?
        (/= num-pending-tasks orig-num-pending-tasks)))))

(defun allocate-intermingle-tasks (slave)
  (with-slots (ordered) slave
    (assert (eq :intermingle ordered))

    ;; Allocate ordered stuff first, then backfill with unordered
    ;; if there is space left over. Since ordered tasks are more
    ;; fragile in that they can only run on a specific slave, we
    ;; schedule them first.
    (let* ((pass-1 (allocate-ordered-tasks slave))
           (pass-2 (allocate-unordered-tasks slave)))
      ;; We don't just or the above two together, because or short circuits
      ;; and we want to run both passes no matter what happens.
      (or pass-1 pass-2))))

(defun allocate-speculating-tasks (slave)
  ;; The algorithm is quite dumb and tries to load balance.  What we
  ;; do is build a list of (task-id num-speculations) pairs, and then
  ;; sort on the num speculations, Then we assign least to most to the
  ;; slave we run out of tasks to allocate over and over until the
  ;; slave has completed its task-group. This will blast many
  ;; speculating tasks into the slaves.

  ;; XXX Currently, however, I'm just going to return nil, and not speculate
  ;; anything until I can get some testing done.
  nil)

;; Go through the allocated task-ids in the slave, and return a list
;; of actual task structures corresponding to the task ids in the
;; slave, _in order of the pending queue in the slave_. These structures
;; are the actual task structures in the taskjar which are the same
;; ones as contained in the taskjar--we do not remove them.
(defun realize-allocation (slave)
  (mapcar #'(lambda (task-id)
              (find-task task-id))
          (car (mw-slave-pending-task-queue slave))))


;; For the given task structure, add a speculation since we're just about to
;; send it to a task for processing.
(defun add-speculation (slave task-id)
  ;; First we associate a speculation entry with the task-id if it
  ;; does not already exist.
  (multiple-value-bind (value present)
      (lkh task-id (mw-taskjar-speculations *taskjar*))
    (unless present
      (skvh task-id
            (make-mw-speculation)
            (mw-taskjar-speculations *taskjar*))))

  ;; Then we add the slave to the speculation hash on the task-id.
  (let ((spec (lkh task-id (mw-taskjar-speculations *taskjar*))))
    (with-slots (slaves) spec
      (emit nil "Adding speculation of task-id ~A on slave ~A~%"
            task-id (mw-slave-sid slave))
      (skvh (mw-slave-sid slave) t slaves))))

;; Returns t if the result we have is the _first_ result for this
;; task-id, nil otherwise. Update the speculation structure so the
;; slave which provided us the answer is no longer present. If no
;; slaves are present, then remove the speculation associated with the
;; task-id altogether.
(defun resolve-speculation (slave task-id have-a-result)
  (with-slots (sid) slave
    ;; find the speculation associated with the task-id
    (let ((spec (lkh task-id (mw-taskjar-speculations *taskjar*))))
      ;; If no spec then 'when' returns nil, which is correct.
      (when spec
        (with-slots (computed-result slaves) spec
          ;; first thing we do is remove the slave for which we are
          ;; resolving the speculation.
          (emit nil "Resolving speculation for slave ~A with task-id ~A~%"
                sid task-id)
          (rkh sid slaves)

          ;; Now, we record if there are any slaves left in the
          ;; speculation
          (let ((spec-count (hash-table-count slaves))
                (is-first-result nil))

            ;; only ever return true from this function if we find the _first_
            ;; result of the speculation.
            (when have-a-result
              (unless computed-result
                (emit nil "Resolving speculation with first result!~%")
                (setf computed-result t
                      is-first-result t)))

            ;; If there are no slaves in the speculation, then we remove the
            ;; speculation structure altogether for the task-id since no slaves
            ;; are working on this task anymore.
            (when (zerop spec-count)
              (emit nil "Removing last speculating slave ~A for task-id ~A~%"
                    sid task-id)
              (rkh task-id (mw-taskjar-speculations *taskjar*)))

            ;; If there aren't anymore speculating slaves, but our
            ;; computed-result is still nil, it simply means that no
            ;; slave had been able to provide us an answer. If the
            ;; computed-result is true, then it means a result had
            ;; been computed at some time, and no slaves are working
            ;; on the task.

            is-first-result))))))

;; Given a task id, how many slaves are speculating on it?
(defun task-num-speculations (task-id)
  (let ((spec (lkh task-id (mw-taskjar-speculations *taskjar*))))
    (if spec
        (hash-table-count (mw-speculation-slaves spec))
        0)))

;; Given a task-id for a task which has been discovered that it needs
;; reclamation, figure out where it should go. If two+ slaves happened
;; to have the same task-id and we are reclaming them, when we figure
;; out where the task-id should go, if the later slave also has a
;; reclamation phase, it is possible for the task to be set into the
;; same set twice. This is ok as only one copy will arise in the set
;; due to hash table semantics.
(defun reclaim-task (task-id &key (was-allocated t))
  (let ((task (find-task task-id)))
    ;; If the task isn't there, it was finished off by another slave,
    ;; we processed it before we got to here, and we're done.
    (when task
      (with-slots (retry do-it-anyway sid) task
        (if retry
            (if sid
                (if do-it-anyway
                    ;; If we are a ordered task, but do-it-anyway
                    ;; is true, this means the task preferred to run
                    ;; on the specific slave, but could really run
                    ;; anywhere. So here we generalize the task-id
                    ;; into unordered set and allow it to run
                    ;; anywhere.  Note: Since speculations must be
                    ;; resolved before we got here, there should be no
                    ;; speculations for the disconnected ordered
                    ;; task.
                    (progn
                      (assert (zerop (task-num-speculations task-id)))
                      (setf sid nil)
                      (skvh task-id task-id
                            (mw-taskjar-unordered-tasks *taskjar*))
                      ;; Since we reclaimed the task and we know there
                      ;; aren't any speculations for it, we ensure it
                      ;; is bookept in accordance with the
                      ;; target-number data.
                      (adjust-pending-target-numbers #'1+ (find-task task-id)))

                    ;; If we are a ordered task, currently only
                    ;; allocatable once, and don't want to redo as an
                    ;; unordered task, we become unrunnable. Rip the
                    ;; task structure out of wherever it lives and
                    ;; punt.
                    (progn
                      (push (remove-task task-id)
                            (mw-taskjar-unrunnable-tasks *taskjar*))
                      ;; Since it has become unrunnable, it will
                      ;; ALWAYS reduce the target-number pending
                      ;; number for that task algo.
                      (when was-allocated
                        (adjust-pending-target-numbers #'1-
                                                       (find-task task-id)))))

                ;; If this was an unordered task, then simply put it
                ;; back into the unordered tasks set to be doled out
                ;; again at a later time. Note there may be other
                ;; active speculations on this task-id. We check to
                ;; see that if there aren't any speculations, then
                ;; this task isn't running anywhere and we perform
                ;; some bookeeping for the target-number structure.
                (progn
                  (skvh task-id task-id
                        (mw-taskjar-unordered-tasks *taskjar*))

                  ;; task isn't running anywhere, so it is considered pending
                  (when was-allocated
                    (when (zerop (task-num-speculations task-id))
                      (adjust-pending-target-numbers #'1+
                                                     (find-task task-id))))))

            ;; If this task is so ephemeral that we don't want to
            ;; retry it when it failed to run on a slave, we check to
            ;; see if there are no other speculations on it, and if
            ;; not, remove it and release it to the gc. If there are
            ;; other speculations, we'll not put this back into any
            ;; set and wait and see if a speculating slave can solve
            ;; it for us. NOTE: We don't even give it back to the
            ;; master algorithm as an unrunnable task.
            (progn
              (when (zerop (task-num-speculations task-id))
                (remove-task task-id)
                (incf (mw-taskjar-num-no-retry-discards *taskjar*))
                ;; We always reduce the pending number because we
                ;; finally know there weren't any speculations active
                ;; for it!
                (adjust-pending-target-numbers #'1- (find-task task-id)))))))))

;; This will go through the ordered table, and for all ordered
;; queues which do not have a connected slave (and hence could not
;; have been currently allocated to any slave), do something with the
;; tasks. Either convert them into unordered tasks, or make them
;; unrunnable.
(defun reclaim-defunct-ordered-tasks ()
  (let ((all-defunct-tasks (make-queue)))
    (maphash
     #'(lambda (sid pending-queue)
         (multiple-value-bind (slave where) (find-slave :sid sid)
           (unless (and slave (intersection '(:connecting :idle :busy)
                                            (list where)))
             ;; Looks like this sid isn't good for use and never will
             ;; be...

             ;; Walk the pending queue and rip them out
             (while (not (empty-queue pending-queue))
               (let ((tid (dequeue pending-queue)))
                 (when tid
                   (enqueue tid all-defunct-tasks))))

             ;; Finally, remove myself (the bad sid) from the hash table.
             ;; ANSI CL says this is ok.
             (rkh sid (mw-taskjar-ordered-tasks *taskjar*)))))
     (mw-taskjar-ordered-tasks *taskjar*))

    ;; Reclaim each of the defunct tasks, we know that all of these
    ;; tasks aren't currently allocated to run and don't alter any
    ;; target-number data concerning pending tasks if they move from
    ;; ordered tasks to unordered tasks.
    (while (not (empty-queue all-defunct-tasks))
      (let ((task-id (dequeue all-defunct-tasks)))
        (reclaim-task task-id :was-allocated nil)))))

;; This will take the task-ids from the disconnected slaves and reclaim
;; them into the taskjar. The tasks could be placed back into whatever
;; set they need to be in, or made unrunnable.
;;
;; XXX arguably, maybe this should go to mw.lisp
(defun reclaim-disconnected-tasks (slave)
  (with-slots (sid pending-task-queue num-pending-tasks) slave
    (let ((task-ids (car pending-task-queue)))
      ;; Even though the slave is disconnected, we'll always leave it in a
      ;; valid state.
      (setf pending-task-queue (make-queue))

      ;; Figure out how to reclaim each task id, which we directly know had
      ;; been allocated to a slave which is now disconnected.
      (mapc #'(lambda (tid)
                (alog t sid "R ~A~%" tid)
                (resolve-speculation slave tid nil)
                (reclaim-task tid :was-allocated t))
            task-ids))))


