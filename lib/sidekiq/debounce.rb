require 'sidekiq/debounce/version'
require 'sidekiq/api'

module Sidekiq
  class Debounce
    def call(worker_class, msg, _queue, redis_pool)
      @worker = worker_class.constantize
      @msg = msg

      return yield unless debounce?

      redis_pool.with do |conn|
        # Get JID of the already-scheduled job, if there is one
        scheduled_jid = conn.get(debounce_key)

        # Reschedule the old job to when this new job is scheduled for
        # Or yield if there isn't one scheduled yet
        if scheduled_jid
          jid = reschedule(scheduled_jid, @msg['at'])
          store_expiry(conn, jid)
          false
        else
          job = yield
          store_expiry(conn, job.fetch('jid'))
          job
        end
      end
    end

    private

    def store_expiry(conn, jid)
      conn.set(debounce_key, jid)
      conn.expireat(debounce_key, @msg['at'].to_i)
    end

    def debounce_key
      hash = Digest::MD5.hexdigest(@msg['args'].to_json)
      @debounce_key ||= "sidekiq_debounce:#{@worker.name}:#{hash}"
    end

    def scheduled_set
      @scheduled_set ||= Sidekiq::ScheduledSet.new
    end

    def reschedule(jid, at)
      job = scheduled_set.find_job(jid)
      job.reschedule(at)
      jid
    end

    def debounce?
      (@msg['at'] && @worker.get_sidekiq_options['debounce']) || false
    end
  end
end
