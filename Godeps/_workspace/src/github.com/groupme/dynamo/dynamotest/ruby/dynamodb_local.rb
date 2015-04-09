require "logger"
require "open3"
require "socket"

# DynamoDBLocal provides a wrapper around the DynamoDB Local service.
#
# Like its golang equivalent, DynamoDBLocal starts and stops an instance of the
# DynamoDB Local service. A port is chosen at random, so multiple services can
# be run concurrently.
#
# A quick how-to:
#
#     require "dynamodb_local"
#     db = DynamoDBLocal.new
#     db.url   # => "http://127.0.0.1:PORT/" (configure your client from this)
#
#     db.start
#     db.wait_until_ready
#
#     db.stop
#     db.wait_until_stopped
#
class DynamoDBLocal
  def initialize(options = {})
    @pid = nil

    if options[:logger]
      @logger = options[:logger]
    else
      $stdout.sync = true
      @logger = Logger.new($stdout)
      @logger.level = Logger::INFO
      @logger.formatter = proc do |severity, datetime, progname, msg|
        "[DynamoDB Local] #{msg}".strip + "\n"
      end
    end
  end

  def url
    "http://127.0.0.1:#{port}/"
  end

  def running?
    return false if @pid.nil?
    begin
      Process.kill(0, @pid)
      true
    rescue Errno::ESRCH
      false
    end
  end

  def ready?
    system("nc -z 127.0.0.1 #{port} > /dev/null")
  end

  def start
    stdin, stdout, stderr, wait_thread = Open3.popen3(command)
    @pid = wait_thread.pid
    info "Running on port #{port} in process #{@pid}"

    stdin.close
    [stdout, stderr].each do |io|
      Thread.new do
        until (line = io.gets).nil? do
          info line
        end
      end
    end
  end

  def stop
    Process.kill("TERM", @pid)
  end

  def wait_until_ready
    sleep 0.05 until ready?
  end

  def wait_until_stopped
    sleep 0.05 while running?
  end

  private

  def command
    dir = File.expand_path(File.join(File.dirname(__FILE__), ".."))
    lib = File.join(dir, "DynamoDbLocal_lib")
    jar = File.join(dir, "DynamoDBLocal.jar")
    "java -Djava.library.path=#{lib} -jar #{jar} -port #{port} -inMemory"
  end

  def port
    return @port if @port

    server = TCPServer.new("127.0.0.1", 0)
    @port = server.addr[1]
    server.close
    @port
  end

  def info(msg)
    @logger.info(msg)
  end
end
