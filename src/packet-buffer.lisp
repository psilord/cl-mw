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

;;;; Packet wire Protocol:
;;;; 1 byte: the number of bytes following which is the integer payload size
;;;; N bytes: MSByte to LSByte ordering, represent payload size in bytes
;;;; X bytes: The serialized payload, controlled by integer in N byte phase.

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; The packet buffer codes
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; XXX This probably does uneccesary copying. Maybe I'll fix it later.
;; Given an array of data, put a version-id and integer spec infront of it
;; and return the whole thing
(defun encode-data-into-packet (data &key (packet-schema-id #x00))
  (let* ((pkt-len (length data))
         (num-bits (integer-length pkt-len))
         ;; Compute how many bytes we need to represent the payload of p
         (num-len-bytes (if (< num-bits 8)
                            1
                            (multiple-value-bind (d m)
                                (floor num-bits 8)
                              (+ d (if (zerop m) 0 1)))))
         ;; used when mucking with ldb
         (bcoeff (- (* num-len-bytes 8) 8)))

    ;; If the number of bytes used to represent the payload length
    ;; integer is larger than 255, we blow up I don't think this will
    ;; ever happen, but you never know.....
    (assert (and (> num-len-bytes 0)
                 (< num-len-bytes 256)))

    ;; The first byte is a schema id number, the second is the number
    ;; of bytes following that constitute an integer of the size of
    ;; the payload, which follows the rest of the way.
    (let ((bytes (make-array (+ 1 1 num-len-bytes pkt-len)
                             :element-type 'unsigned-byte)))

      ;; The first element is the schema type of the packet
      (setf (aref bytes 0) packet-schema-id)

      ;; The second element is the size of the integer in bytes
      (setf (aref bytes 1) num-len-bytes)

      ;; Compute the MSB to LSB encoding of the integer and write into
      ;; bytes array
      (do ((n bcoeff (- n 8))
           (idx 2 (1+ idx))
           (written 0 (1+ written)))
          ((= written num-len-bytes) nil)
        (setf (aref bytes idx) (ldb (byte 8 n) pkt-len)))

      ;; Copy over the rest of the data into the packet.  Returns the
      ;; bytes sequence, possibly as a new array, but that's ok since
      ;; it is what the caller uses.
      (replace bytes data
               :start1 (+ 2 num-len-bytes)
               :start2 0 :end2 pkt-len))))


;; Read and return the integer encoded from MSB to LSB in the array
;; are from idx knowing it is len long.
(defun read-packet-integer (ar index len)
  ;; Going from MSB to LSB, we add in the byte, then shift the
  ;; sum left one byte, then do it again until done. This
  ;; reproduces the integer again.
  (do ((idx index (1+ idx))
       (bytes-read 0 (1+ bytes-read))
       (sum 0 (+ (ash sum 8) (aref ar idx))))
      ((= bytes-read len) sum)))


;; We believe there is a full packet to rip out of the array v at the offset.
(defun decode-packet-into-data (v offset)
  (let* ((schema-id (aref v (+ offset 0)))
         (num-len-bytes (aref v (+ offset 1)))
         (pkt-len (read-packet-integer v (+ offset 2) num-len-bytes)))

    ;; For now we only support this schema.
    (assert (= schema-id #x00))

    ;; Make a brand new array, copy only the payload into it, and
    ;; return it.  replace is destructive, but I return the value of
    ;; it which the caller uses.
    (let ((data (make-array pkt-len :element-type 'unsigned-byte))
          (data-start-index (+ offset (+ 1 1 num-len-bytes))))
      (replace data v
               :start1 0
               :start2 data-start-index
               :end2 (+ data-start-index pkt-len)))))

;; A read-packet-buffer is use only for reading a packet from the other side.
;; It never writes to the other side, only reads from it.
(defun make-read-packet-buffer (socket disconnector
                                &key
                                (initial-buffer-size
                                 (* 1024 1))
                                (buffer-growth-function
                                 #'(lambda (cur-size max-size)
                                     (max max-size (floor (* cur-size 2)))))
                                (max-buffer-size
                                 (* initial-buffer-size 16)))
  (let*
      ;; DNS or ip address of the other side
      ((who (remote-host socket))
       ;; Remote port of the other side
       (port (remote-port socket))

       ;; Am I registred with the multiplexer and acepting data?
       (accepting-data t)

       ;; The buffer allocated to read a response from the other
       ;; side. If the size of the buffer eventually exceeds
       ;; max-buffer-size, close the connection to the other client
       ;; and call the terminated function with the reason why. We
       ;; can't store infinite amounts of data, especially with tens
       ;; of thousands of clients.
       (buf (make-array initial-buffer-size
                        :adjustable t :element-type 'unsigned-byte))
       ;; Where is the index into the buffer at which we will start
       ;; recording the raw data from the other side?
       (read-index 0)

       ;; There may be more than one packet in the buffer if the other
       ;; side has sent multiple packets in a row. So we carefully
       ;; trace through the packet data looking for completed packets,
       ;; handing them off to the notify change function, and
       ;; continuing to read. If we run out of space to read
       ;; something, we byte shift the unfinished packet back to the
       ;; beginning of the buffer and keep reading. If we simply run
       ;; out of space (the packet is larger than the maximum
       ;; allowable buffer sizse), we close the connection to the
       ;; other side, and inform the notify change function.
       ;;
       ;; This is the index of a known start of a packet in the buffer.
       ;; It stays this index until it can move to the next known start of
       ;; a packet.
       (packet-start 0)
       ;; As we inspect each portion of the packet from the packet start, we
       ;; keep track of what it is we are parsing. This lets me know how to
       ;; figure out if I have enough of the bytes in the state to look at
       ;; it properly.
       ;; States are :schema, :length-spec, :length, :payload
       (current-state :schema)
       ;; When I enter a state, I need at least this many bytes read
       ;; in order have enough bytes to inspect it and process that
       ;; region of the packet. This is directly related to the binary
       ;; layout of the packet on the wire. We initialize it to 1
       ;; since that is how many bytes I need to read the schema byte.
       (need-at-least 1)
       ;; This represents how many bytes I've read while in a
       ;; particular state.
       (cumulative-bytes-in-current-state 0)
       ;; As I'm processing the packet, picking things out of it, this
       ;; represents my inspection pointer as a delta index from the
       ;; packet-start.
       (offset-index 0)

       ;; For the current packet I'm processing, this represents what
       ;; I've found.
       (schema-id 0)
       (payload-length-spec 0)
       (payload-length 0)

       ;; When a packet is completely read, we copy the payload out of
       ;; it and pass it to the function associated with this
       ;; binding. Also, if EOF was seen or any other network error,
       ;; pass that information along as well.
       ;;
       ;; We'll call the notify-change handler with a bunch of
       ;; information including the following. Please see the function
       ;; call-notify-change to see exactly what is passed.
       ;;
       ;; :packet <packet>
       ;; :eof
       ;; :problem <why: :short-read, :packet-too-big, :reset, :unknown>.
       (notify-change nil))

    ;; The local functions, some of which we shall export out of the closure

    (labels ((controller (&rest actions)
               ;; A closure which can be used externally to do
               ;; something with this buffer, like remove a handler,
               ;; shutdown this side of the connection, or close the
               ;; connection.
               (apply disconnector actions))

             (call-notify-change (cmd &rest args)
               ;; If the notify-change handler wants the buffer to
               ;; stop procesing anything RIGHT NOW, then honor it.
               (let ((future (apply notify-change
                                    `(,who ,port ,socket ,#'controller ,cmd
                                           ,@args))))
                 (ecase future
                   (:abort
                    (throw :finished t))
                   (:continue
                    t))))

             (slide-buffer ()
               ;; As packet-start approaches the end of the buffer we
               ;; might not have enough space to read the bytes we
               ;; need. So, this function slides the data in the
               ;; buffer to the beginning of the buffer and updates
               ;; all of the indicies and whatnot so noone is the
               ;; wiser. Returns true if a slide took place, nil if
               ;; not.

               (when (= packet-start 0)
                 (emit nil "Need to slide buffer back to start...no~%")
                 (return-from slide-buffer nil))

               (emit nil "Need to slide buffer back to start...yes [shift ~
                        ~A bytes by ~A places]~%"
                     (- read-index packet-start) packet-start)

               ;; Slide everything from packet-start to read-index back to
               ;; index zero in the buffer.
               (replace buf buf :start1 0
                        :start2 packet-start
                        :end2 read-index)

               ;; adjust the indicies so we don't screw stuff up after a slide!
               (decf read-index packet-start)
               (setf packet-start 0)
               t)

             (resize-buffer ()
               ;; If we've determined the buffer is too small to hold
               ;; the incoming data, then make it bigger preserving
               ;; the contents.  Return t if the resize was ok, nil if
               ;; the resize would take it beyond the limits allowed.
               (let ((new-size
                      (funcall buffer-growth-function
                               (length buf) max-buffer-size)))
                 (emit nil "Resizing buffer from ~A to ~A~%"
                       (length buf) new-size)
                 (when (> new-size max-buffer-size)
                   (emit nil "Oops, not really, packet too big!~%")
                   (return-from resize-buffer nil))
                 ;; This keeps everything aligned in the buffer, it is
                 ;; just bigger.
                 (setf buf (adjust-array buf new-size
                                         :element-type 'unsigned-byte
                                         :initial-element 0))))
             (consume-packets ()
               ;; Knowing how much we need to have read for each
               ;; portion of the packet, process the bytes until we
               ;; can complete a packet or know that we need more
               ;; bytes. XXX This is a hilariously crappy piece of
               ;; code. Sorry.
               (emit nil "In consume-packets: cum=~A nal=~A~%"
                     cumulative-bytes-in-current-state need-at-least)

               (while (>= cumulative-bytes-in-current-state
                          need-at-least)

                 (emit nil "Processing packet state ~S!~%" current-state)
                 ;; Since we can consume many states in this loop,
                 ;; if we consume a state but still had read much
                 ;; more initially, we view the remaining read
                 ;; byes as having happened in the next state.
                 (decf cumulative-bytes-in-current-state
                       need-at-least)

                 (case current-state

                   ;; The schema-id is a single byte quantity
                   ((:schema)
                    ;; The schema-id is the byte at the packet start
                    ;; and offset-index starts at the packet start
                    (setf schema-id
                          (aref buf (+ packet-start offset-index)))

                    ;; This is the only packet schema we handle right
                    ;; now. If we don't like the schema id, then
                    ;; inform the notify-change handler about it who
                    ;; may, and probably will, close the client
                    ;; connection.
                    (unless (= schema-id #x00)
                      (call-notify-change :read-problem :bad-schema schema-id))

                    (emit nil "Schema id is: ~A~%" schema-id)
                    (emit nil "packet-start index = ~A~%" packet-start)

                    ;; Consume the schema-id
                    (incf offset-index 1)

                    ;; set up the next state, which is the :length-spec
                    ;; I know I need one byte for it.
                    (setf need-at-least 1)
                    (setf current-state :length-spec))

                   ;; The length-spec is a single byte quantity
                   ((:length-spec)

                    ;; the payload-length-spec tells me how
                    ;; many following bytes are going to be
                    ;; the integer length of the payload
                    (setf payload-length-spec
                          (aref buf (+ packet-start offset-index)))
                    (emit nil "Payload length spec is: ~A~%" payload-length-spec)

                    ;; Consume the payload length spec
                    (incf offset-index 1)
                    ;; set up the next state, which is the :length
                    ;; state. I know I need payload-length-spec bytes
                    (setf need-at-least payload-length-spec)
                    (setf current-state :length))

                   ;; The length is a number of bytes
                   ;; describing an integer.
                   ((:length)

                    ;; the payload-length-spec tells me how
                    ;; many following bytes are going to be
                    ;; the integer length of the payload
                    (setf payload-length
                          (read-packet-integer buf
                                               (+ packet-start
                                                  offset-index)
                                               payload-length-spec))
                    (emit nil "Payload length is: ~A~%" payload-length)

                    ;; Consume the payload length
                    (incf offset-index payload-length-spec)
                    ;; set up the next state, which is the :length
                    ;; state. I know I need payload-length-spec bytes
                    (setf need-at-least payload-length)
                    (setf current-state :payload))

                   ;; When the payload is finally read, I can
                   ;; consume the whole thing, move it along
                   ;; to the notify-change function and move
                   ;; the packet-start to the next packet
                   ;; beginning.
                   ((:payload)
                    ;; figure out the end of the packet
                    (incf offset-index payload-length)
                    ;; Rip the packet out of the buffer and give it to
                    ;; the notify-change handler. The chunk of data
                    ;; given to the nitify-change handler is newly
                    ;; allocated.
                    (call-notify-change :read-packet
                                        (decode-packet-into-data buf
                                                                 packet-start))
                    ;; Move the packet-start there
                    (incf packet-start offset-index)
                    ;; reset the offset from the packet-start
                    (setf offset-index 0)
                    ;; About to read the schema id of the next packet
                    (setf need-at-least 1)
                    (setf current-state :schema))))

               ;; Return true if we are sitting at a packet boundary,
               ;; ready to read the next packet. Return nil if we are
               ;; in the middle of reading a packet and need more
               ;; bytes. We use this to know if there was an
               ;; unfinished packet in the buffer when the other side
               ;; went away.
               (if (eq current-state :schema)
                   t
                   nil))

             (drain-buffers ()
               ;; If any complete packets are left in the
               ;; buffer, consume them and fire them off
               ;; to the notify-change handler
               (if (consume-packets)
                   ;; all packets read and processed
                   (call-notify-change :read-eof)
                   ;; a partial packet is left at network boundary.
                   ;; we end up dumping the information iside of it, but the
                   ;; notify-change function knows this happened.
                   (call-notify-change :read-problem :short-read)))

             (read-some-bytes (fd event exception)
               ;; This is the read handler registered to the IOLib
               ;; multiplexer.  It is responsible for reading packets
               ;; in a non-blocking fashion and upon their completion
               ;; handling them off to a callback.
               (catch :finished
                 (handler-case
                     (progn

                       ;; First, handle if we got a timeout on the read.
                       (when (eq exception :timeout)
                         ;; The return value of this call will
                         ;; probably ask us to bail us out of the
                         ;; read-some-bytes function.
                         (call-notify-change :read-timeout
                                             (get-universal-time)))


                       ;; If no timeout, then read however much we can.
                       (multiple-value-bind (rbuf bytes-read)
                           (receive-from socket
                                         :buffer buf
                                         :start read-index
                                         :end (length buf))
                         (emit nil "Read ~A bytes from ~A:~A~%"
                               bytes-read who port)

                         ;; After reading from the other side, we
                         ;; eagerly scrape the buffer for completed
                         ;; packets. So, if we get an EOF here, it wil
                         ;; only be after a full packet has been read
                         ;; and processed (and the other side went
                         ;; away), or the other side had an incident and
                         ;; went away in mid packet. Either way, there
                         ;; should be no fully completed packets in the
                         ;; buffer when we get an eof.
                         (when (zerop bytes-read)
                           (error 'end-of-file))

                         ;; This will be where we start recording our
                         ;; bytes next.
                         (incf read-index bytes-read)

                         ;; Keep track of how much we read in the current state
                         (incf cumulative-bytes-in-current-state bytes-read)

                         ;; If there are any complete packets in the
                         ;; buffer, suck them out and send them on
                         ;; their way
                         (consume-packets)

                         ;; If we determine that we don't have enough
                         ;; space here to read need-at-least bytes, we
                         ;; try and slide everything to the beginning
                         ;; of the buffer. If there is no room to
                         ;; slide, and we have no room to read, we
                         ;; resize the buffer according to the growth
                         ;; function and try again. If we hit the
                         ;; limit of the buffer after expansion and
                         ;; still yet more needs to be read to satisfy
                         ;; need-at-least, we declare the other side
                         ;; and inform the notify-change function of it.
                         (when (>= (+ read-index need-at-least)
                                   (length buf))

                           ;; Try and slide the buffer and do another
                           ;; go around reading more bytes.
                           (unless (slide-buffer)
                             ;; Try and resize buffer and do another
                             ;; go around reading some bytes.
                             (unless (resize-buffer)
                               ;; Hrm, the slide failed and we can't
                               ;; pass the resize limit we set. Looks
                               ;; like the other side wrote too much
                               ;; to us.
                               (call-notify-change :read-problem
                                                   :packet-too-big))))))


                   ;; All of our exceptional cases are about the
                   ;; same. Finish ripping out completed packets.
                   (end-of-file ()
                     (emit nil "Got EOF from client!~%")
                     (drain-buffers))

                   (socket-connection-reset-error ()
                     (emit nil "Got reset from client!~%")
                     (drain-buffers))))))

      #'(lambda (msg &rest args)
          (ecase msg

            (:halt-accepting-bytes
             ;; It could be that the client is sending us so many
             ;; packets so quickly that we can't deal with it, so
             ;; unregister the read handler which effectively blocks
             ;; the client.
             (when accepting-data
               (funcall disconnector :read))
             (setf accepting-data nil))

            (:resume-accepting-bytes
             ;; When we're ready to handle more packets from the
             ;; client, we re-register the handler which most likely
             ;; unblocks the client.
             (unless accepting-data
               (set-io-handler *event-base* (socket-os-fd socket)
                               :read
                               #'read-some-bytes)
               (setf accepting-data t)))

            (:accepting-data
             accepting-data)

            (:shutdown-read
             ;; We don't want any more data from this client, or we've
             ;; seen EOF from it and know we aren't getting
             ;; anymore. Shutdown the read end of the other side.
             (funcall disconnector :shutdown-read))

            (:notify-change-func
             (assert (and args (functionp (car args))))
             (setf notify-change (car args)))

            (:controller
             #'controller)

            (:read-some-bytes
             #'read-some-bytes))))))


;; A write-packet-buffer is used only for writing packets to the other
;; side.  It never reads from the other side, only sends to it.  If
;; there is nothing to write, it removes itself from the multiplexer
;; to stop uneeded handling since writing to a client is almost always
;; possible. It reregisters itself when there is something to write
;; and fires off some bytes outside of the multiplexer just in case it
;; can to avoid a go around with the multiplexer.
(defun make-write-packet-buffer (socket disconnector
                                 &key (max-pending (* 1024 32)))
  (let
      ;; DNS or ip address of the other side
      ((who (remote-host socket))
       ;; Remote port of the other side
       (port (remote-port socket))

       ;; This holds packets that I need to send.
       (packet-queue (make-queue))
       ;; And the total size of them.
       (pending-bytes-to-write 0)

       ;; This is the active packet I'm currently sending to the other side.
       (packet-to-send nil)
       ;; Where is the current write index of the packet I'm sending.
       ;; When it reaches the end of the packet, the whole thing is sent.
       ;; Then I can be ready for more data to send. We always try to write
       ;; the whole rest of the buffer.
       (write-index 0)

       ;; Am I registred with the multiplexer?
       (sending-data nil)

       ;; Someone wants to get a message when we've flushed all of our data
       ;; to the other side.
       (flushing nil)

       ;; This is the callback function stating we either finished writing the
       ;; information, or some other problem happened.
       (notify-change nil))

    (labels ((controller (&rest actions)
               (apply disconnector actions))

             (call-notify-change (cmd &rest args)
               ;; If the notify-change handler wants the buffer to
               ;; stop procesing anything RIGHT NOW then honor it.
               (unless
                   (apply notify-change
                          `(,who ,port ,socket ,#'controller ,cmd ,@args))
                 (throw :finished t)))

             (ready-to-accept-data ()
               ;; This is advisory, if it is ignored, we'll just accept the
               ;; packets for sending anyway...
               (< pending-bytes-to-write max-pending))

             (send-data (&rest data)
               ;; This function is exported to the outside so the
               ;; application can send a pile of data to the other
               ;; side.  We accept a pile of data buffers, wrap them
               ;; up into packets, and enqueue them to make them ready
               ;; for sending.
               ;;
               ;; XXX I should probably use a ring buffer for this
               ;; like the read buffer, otherwise I'll get a lot of
               ;; small writes to the other side.
               (unless sending-data
                 (set-io-handler *event-base*
                                 (socket-os-fd socket)
                                 :write
                                 #'write-some-bytes)
                 (setf sending-data t))

               ;; encode all the data buffers into packets and queue them up
               (dolist (x data)
                 (let ((packet (encode-data-into-packet x)))
                   (enqueue packet packet-queue)
                   (incf pending-bytes-to-write (length packet))))

               (emit nil "Pending bytes is ~A.~%" pending-bytes-to-write))

             (write-some-bytes (fd event exception)
               ;; While there is data to write, the multiplexer is
               ;; going to call me back so I can write it. When I'm
               ;; done writing it, I'll deregister myself until I get
               ;; something else to write.
               (emit nil "write-some-bytes called!~%")
               (catch :finished
                 (handler-case
                     (progn
                       ;; Do something special if I've been asked to flush
                       (when flushing
                         (unless (or packet-to-send (peek-queue packet-queue))
                           ;; we must do this first before calling the
                           ;; notify change function, which might
                           ;; close me and remove everything.
                           (funcall disconnector :write)
                           (setf sending-data nil)
                           (call-notify-change :writes-flushed)
                           (setf flushing nil)
                           (throw :finished t)))

                       ;; If there is no packet we're currently
                       ;; sending, get one from the queue.
                       (unless packet-to-send
                         (setf packet-to-send (dequeue packet-queue))
                         ;; And our algorithm better not think we
                         ;; should have been writing any packets when
                         ;; there aren't any!
                         (assert packet-to-send))

                       ;; Write as much of the packet as we can
                       (when (/= write-index (length packet-to-send))
                         (let ((wrote-bytes
                                (send-to socket packet-to-send
                                         :start write-index
                                         :end (length packet-to-send))))
                           (emit nil "Wrote ~A bytes to client: ~A:~A~%"
                                 wrote-bytes who port)

                           (incf write-index wrote-bytes)
                           (decf pending-bytes-to-write wrote-bytes)))

                       (when (= write-index (length packet-to-send))
                         ;; We've sent the whole packet, call
                         ;; the notify handler, and get ready for the next
                         ;; packet.
                         (setf packet-to-send nil)
                         (setf write-index 0)
                         ;; And we inform the user of us that we are done!
                         (call-notify-change :write-finished))

                       ;; If we wrote the last of the queued packet
                       ;; data, unregister ourselves so we don't tax
                       ;; the multiplexer by having it call us all the
                       ;; time when we have nothing to do.
                       (when (zerop pending-bytes-to-write)
                         (funcall disconnector :write)
                         (setf sending-data nil)
                         (when flushing
                           (setf flushing nil)
                           ;; We were requested to send a message when all of
                           ;; our writes were sent.
                           (call-notify-change :writes-flushed))))

                   (socket-connection-reset-error ()
                     ;; Client went away, or closed its reading end, either
                     ;; way it isn't going to be accepting more data.
                     (call-notify-change :write-problem :reset))

                   (isys:ewouldblock ()
                     ;; Do nothing, we'll just let the multiplexer call us back
                     ;; later when stuff is ready to write.
                     nil)

                   (isys:epipe ()
                     ;; The client went away while writing to
                     ;; it. Probably not good.
                     (call-notify-change :write-problem :hangup))))))

      ;; The function by which we interact with the closure.
      #'(lambda (msg &rest args)
          (ecase msg

            (:send-data
             ;; Give me more data to send to the other side.
             (apply #'send-data args))

            (:notify-change-func
             ;; When I finish packets or there is a problem, call this.
             (assert (and args (functionp (car args))))
             (setf notify-change (car args)))

            (:controller
             ;; If someone needs to control me directly, here is the function
             ;; needed for it.
             #'controller)

            (:ready
             ;; Return whether or not we are ready to receive more data.
             (ready-to-accept-data))

            (:flush
             ;; Whoever is using us wants us to send a message when all of the
             ;; data has been written.
             (unless sending-data
               (set-io-handler *event-base*
                               (socket-os-fd socket)
                               :write
                               #'write-some-bytes)
               (setf sending-data t))
             (setf flushing t))

            (:write-some-bytes
             #'write-some-bytes))))))


;; This abstracts the read and write packet buffer codes into one
;; interface for ease of use.
;;
;; XXX As of yet, there is no use for max-write-buffer-size since I don't
;; process the write buffers with that idea in mind yet. It is for a future
;; coalescing of symmetry into the read/write packet buffers.
(defun make-packet-buffer (socket disconnector
                           &key
                           (max-read-buffer-size (* 1024 128)))
  (let ((read-pkt-buf (make-read-packet-buffer
                       socket disconnector
                       :max-buffer-size max-read-buffer-size))
        (write-pkt-buf (make-write-packet-buffer socket disconnector))

        ;; This is the notify change function out caller gave us, what
        ;; we'll do is register our own copy of the notify change
        ;; function with the appropriate reader, then alter the
        ;; controller function we get passed to be the one from this
        ;; closure instead. This way the notify change handler can
        ;; alter both the read and write packet buffer.
        (read-notify-change nil)
        (write-notify-change nil))

    (labels ((master-read-notify-change (who port client controller
                                             cmd &rest args)
               ;; Convert the controller given back to us to the one
               ;; which allows controlling of both the packet buffers.
               ;; We also export the cmd keywords, which need to be
               ;; unique between the read and write buffers
               (apply read-notify-change `(,who ,port ,client
                                                ,#'master-controller
                                                ,cmd
                                                ,@args)))

             (master-write-notify-change (who port client controller
                                              cmd &rest args)
               ;; Convert the controller given back to us to the one
               ;; which allows controlling of both the packet buffers.
               ;; We also export the cmd keywords, which need to be
               ;; unique between the read and write buffers
               (apply write-notify-change `(,who ,port ,client
                                                 ,#'master-controller
                                                 ,cmd
                                                 ,@args)))

             (master-controller (which &rest args)
               ;; Through this function one can have full access to the
               ;; underlying packet buffers, in addition to closing the
               ;; connection quickly.
               (ecase which
                 (:read-notify-change-func
                  ;; We secretly set up out own handler to convert the
                  ;; controller argument to be for both packet
                  ;; buffers. Eventually, we end up calling the user's
                  ;; function.
                  (assert (and args (functionp (car args))))
                  (setf read-notify-change (car args))
                  (funcall read-pkt-buf
                           :notify-change-func #'master-read-notify-change))

                 (:write-notify-change-func
                  ;; We secretly set up out own handler to convert the
                  ;; controller argument to be for both packet
                  ;; buffers. Eventually, we end up calling the user's
                  ;; function.
                  (assert (and args (functionp (car args))))
                  (setf write-notify-change (car args))
                  (funcall write-pkt-buf
                           :notify-change-func #'master-write-notify-change))

                 (:disconnector
                  ;; A raw interface to the disconnector for both buffers.
                  (apply disconnector args))

                 ;; I can ask the reader and writer individual things...
                 (:reader
                  (apply read-pkt-buf args))

                 (:writer
                  (apply write-pkt-buf args))

                 (:read-some-bytes
                  (funcall read-pkt-buf :read-some-bytes))

                 (:write-some-bytes
                  (funcall write-pkt-buf :write-some-bytes)))))

      ;; the function we return to the creator of this packet buffer.
      #'master-controller)))






