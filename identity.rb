# frozen_string_literal: true

require 'redis'
require 'securerandom'

conn = Redis.new

def new_id
  SecureRandom.uuid
end

def new_ts
  (Time.now + (rand(1..10) * rand(1..24) * rand(1..60) * rand(1..60))).to_i
end

def random_score
  rand(1..100)
end

# User holds user specific data.
class User
  attr_accessor :id, :email, :score, :created_at, :updated_at

  def initialize
    @id = new_id
    @created_at = new_ts
    @updated_at = new_ts
  end

  def to_s
    "id: #{id}\t score: #{score}, created_at: #{created_at}\t updated_at: #{updated_at}\t email: #{email}"
  end
end

def prepare_scores(conn, app_id)
  conn.zrevrange("identity:oauthapp:#{app_id}:index:users:created_at", 0, -1).each do |id|
    conn.hset("identity:user:#{id}:sorting", 'risk_score', random_score)
  end
end

def load_user(conn, id)
  data = conn.hgetall("identity:user:#{id}")
  user = User.new
  user.id = id
  user.email = data['email']
  user.created_at = data['created_at']
  user.updated_at = data['updated_at']

  risk_score = conn.hget("identity:user:#{id}:sorting", 'risk_score')
  user.score = risk_score

  user
end

def print(users)
  users.each do |user|
    puts user
  end
end

def list_app_user_by_created_at(conn, app_id, direction)
  users = []
  conn.sort("identity:oauthapp:#{app_id}:index:users:created_at", order: direction, by: 'identity:user:*->created_at').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_updated_at(conn, app_id, direction)
  users = []
  conn.sort("identity:oauthapp:#{app_id}:index:users:created_at", order: direction, by: 'identity:user:*->updated_at').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_email(conn, app_id, direction)
  users = []
  conn.sort("identity:oauthapp:#{app_id}:index:users:created_at", order: "alpha #{direction}", by: 'identity:user:*->email').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_score(conn, app_id, direction)
  users = []
  conn.sort("identity:oauthapp:#{app_id}:index:users:created_at", order: "#{direction}", by: 'identity:user:*:sorting->risk_score').each do |id|
    user = load_user(conn, id)
    users << user
  end
  users
end

def list_app_user_by_joined_at(conn, app_id, direction)
  users = []
  result = if direction == 'desc'
             conn.zrevrange("identity:oauthapp:#{app_id}:index:users:created_at", 0, -1)
           else
             conn.zrange("identity:oauthapp:#{app_id}:index:users:created_at", 0, -1)
           end
  result.each do |id|
    users << load_user(conn, id)
  end
  users
end

def list_app_user(conn, app_id, options)
  if options[:sort_by] == ''
    list_app_user_by_joined_at(conn, app_id, options[:direction])
  elsif options[:sort_by] == 'created_at'
    list_app_user_by_created_at(conn, app_id, options[:direction])
  elsif options[:sort_by] == 'updated_at'
    list_app_user_by_updated_at(conn, app_id, options[:direction])
  elsif options[:sort_by] == 'email'
    list_app_user_by_email(conn, app_id, options[:direction])
  elsif options[:sort_by] == 'score'
    list_app_user_by_score(conn, app_id, options[:direction])
  end
end

app_id = '0c6c0d2e-7665-48ed-b923-cd77c0a48c8f'

puts 'Preparing user scores ...'
prepare_scores(conn, app_id)

puts 'Order by joined_at ...'
print(list_app_user(conn, app_id, { 'sort_by': '', 'direction': 'asc' }))

puts 'Order by created_at ...'
print(list_app_user(conn, app_id, { 'sort_by': 'created_at', 'direction': 'asc' }))

puts "\nOrder by updated_at ..."
print(list_app_user(conn, app_id, { 'sort_by': 'updated_at', 'direction': 'asc' }))

puts "\nOrder by email ..."
print(list_app_user(conn, app_id, { 'sort_by': 'email', 'direction': 'desc' }))

puts "\nOrder by score ..."
print(list_app_user(conn, app_id, { 'sort_by': 'score', 'direction': 'desc' }))
