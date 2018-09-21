defmodule Redix.PubSub.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils
  alias Redix.ConnectionError

  require Logger

  @compile { :ms_pattern }

  defstruct opts: nil,
            socket: nil,
            continuation: nil,
            backoff_current: nil,
            # A dictionary of `channel => recipient_pids` where `channel` is either
            # `{:channel, "foo"}` or `{:pattern, "foo*"}` and `recipient_pids` is a
            # map of pids of recipients to their monitor ref for that
            # channel/pattern.
            subscriptions: nil

  @backoff_exponent 1.5

  ## Callbacks

  def init(opts) do
    state = %__MODULE__{opts: opts,
                        subscriptions: :ets.new(:subscriptions, [:duplicate_bag,
                                                                 write_concurrency: true,
                                                                 read_concurrency: true])}
    
    if opts[:sync_connect] do
      sync_connect(state)
    else
      {:connect, :init, state}
    end
  end

  def connect(info, state) do
    case establish_connection(state.opts) do
      {:ok, socket} ->
        state = %{state | socket: socket}

        if info == :backoff do
          log(state, :reconnection, ["Reconnected to Redis (", Utils.format_host(state), "?)"])

          case resubscribe_after_reconnection(state) do
            :ok ->
              {:ok, state}
            {:error, reason} ->
              {:disconnect, {:error, %ConnectionError{reason: reason}}, state}
            nil ->
              {:ok, state}
          end
        else
          {:ok, state}
        end

      {:error, reason} ->
        log(state, :failed_connection, [
          "Failed to connect to Redis (",
          Utils.format_host(state),
          "): ",
          Exception.message(%ConnectionError{reason: reason})
        ])

        next_backoff =
          calc_next_backoff(
            state.backoff_current || state.opts[:backoff_initial],
            state.opts[:backoff_max]
          )

        if state.opts[:exit_on_disconnection] do
          {:stop, reason, state}
        else
          {:backoff, next_backoff, %{state | backoff_current: next_backoff}}
        end

      {:stop, reason} ->
        {:stop, reason, state}
    end
  end

  def disconnect({:error, %ConnectionError{reason: reason} = error}, state) do
    log(state, :disconnection, [
      "Disconnected from Redis (",
      Utils.format_host(state),
      "): ",
      ConnectionError.message(error)
    ])

    :ok = :gen_tcp.close(state.socket)

    if state.opts[:exit_on_disconnection] do
      {:stop, reason, state}
    else
      :ets.foldl(fn ({channel, {r, p}}, acc) ->
        send(p, message(:disconnected, %{error: error}))
      end, nil, state.subscriptions)
      
      state = %{
        state
        | socket: nil,
          continuation: nil,
          backoff_current: state.opts[:backoff_initial]
      }

      {:backoff, state.opts[:backoff_initial], state}
    end
  end

  def handle_cast({operation, targets, subscriber}, state)
      when operation in [:subscribe, :psubscribe] do
    register_subscription(state, operation, targets, subscriber)
  end

  def handle_cast({operation, channels, subscriber}, state)
      when operation in [:unsubscribe, :punsubscribe] do
    register_unsubscription(state, operation, channels, subscriber)
  end

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)
    state = new_data(state, data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, %ConnectionError{reason: :tcp_closed}}, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, %ConnectionError{reason: reason}}, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, %{subscriptions: subscriptions} = state) do
    ms = [{{:"$1", {:"$2", :"$3"}}, [{:==, :"$3", pid}], [:"$_"]}]
    :ets.select_delete subscriptions, ms
    {:noreply, state}
  end

  ## Helper functions

  defp sync_connect(state) do
    case establish_connection(state.opts) do
      {:ok, socket} ->
        {:ok, %{state | socket: socket}}

      {:error, reason} ->
        {:stop, reason}

      {:stop, _reason} = stop ->
        stop
    end
  end

  defp establish_connection(opts) do
    with {:ok, socket} <- Utils.connect(opts),
         :ok <- :inet.setopts(socket, active: :once) do
      {:ok, socket}
    end
  end

  defp new_data(state, <<>>) do
    state
  end

  defp new_data(state, data) do
    case (state.continuation || &Protocol.parse/1).(data) do
      {:ok, resp, rest} ->
        state = handle_pubsub_msg(state, resp)
        new_data(%{state | continuation: nil}, rest)

      {:continuation, continuation} ->
        %{state | continuation: continuation}
    end
  end

  defp register_subscription(%{subscriptions: subscriptions} = state, kind, targets, subscriber) do
    msg_kind =
      case kind do
        :subscribe -> :subscribed
        :psubscribe -> :psubscribed
      end

    Enum.map(targets, fn channel ->
      ms = [{{:"$1", {:"$2", :"$3"}}, [{:==, :"$1", channel}, {:==, :"$3", subscriber}], [:"$_"]}]
      has_channel = case :ets.select subscriptions, ms do
                      [] ->
                        :ets.insert(subscriptions, {channel, {Process.monitor(subscriber), subscriber}})
                        false
                      xs -> true
                    end
      send(subscriber, message(msg_kind, %{:channel => channel}))
      unless has_channel do
        redis_command =
          case kind do
            :subscribe -> "SUBSCRIBE"
            :psubscribe -> "PSUBSCRIBE"
          end

        command = Protocol.pack([redis_command | [channel]])
        send_noreply_or_disconnect(state, command)
      end
    end)

    {:noreply, state}
  end

  defp register_unsubscription(%{subscriptions: subscriptions} = state, kind, targets, subscriber) do
    msg_kind =
      case kind do
        :unsubscribe -> :unsubscribed
        :punsubscribe -> :punsubscribed
      end
    Enum.map(targets, fn channel ->
      ms = [{{:"$1", {:"$2", :"$3"}}, [{:==, :"$1", channel}, {:==, :"$3", subscriber}], [:"$_"]}]
      to_delete = :ets.select subscriptions, ms
      Enum.each(to_delete, fn ({channel, {ref, pid}}) ->
        Process.demonitor(ref)
        send(pid, message(msg_kind, %{:channel => channel}))
        :ets.match_delete subscriptions, {channel, {ref, pid}}
      end)
    end)

    {:noreply, state}
  end

  defp handle_pubsub_msg(state, [operation, _target, _count])
  when operation in ~w(subscribe psubscribe unsubscribe punsubscribe) do
    state
  end

  defp handle_pubsub_msg(%{subscriptions: subscriptions} = state, ["message", channel, payload]) do
    message = message(:message, %{channel: channel, payload: payload})

    case :ets.lookup(subscriptions, channel) do
      [] -> Logger.warn "{:channel, #{channel}} was not subscribed."
      xs ->
        Enum.each(xs, fn {c, {ref, pid}} -> send(pid, message) end)
    end

    state
  end

  defp handle_pubsub_msg(%{subscriptions: subscriptions} = state, [
         "pmessage",
         pattern,
         channel,
         payload
       ]) do
    message = message(:pmessage, %{channel: channel, pattern: pattern, payload: payload})

    case :ets.lookup(subscriptions, channel) do
      [] -> Logger.warn "{:channel, #{channel}} was not subscribed."
      xs -> Enum.each(xs, fn {c, {ref, pid}} -> send(pid, message) end)
    end

    state
  end

  defp calc_next_backoff(backoff_current, backoff_max) do
    next_exponential_backoff = round(backoff_current * @backoff_exponent)

    if backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, backoff_max)
    end
  end

  defp message(kind, properties) when is_atom(kind) and is_map(properties) do
    {:redix_pubsub, self(), kind, properties}
  end

  defp send_noreply_or_disconnect(%{socket: socket} = state, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, state}

      {:error, reason} ->
        {:disconnect, {:error, %ConnectionError{reason: reason}}, state}
    end
  end

  defp resubscribe_after_reconnection(%{subscriptions: subscriptions} = state) do
    channels = :ets.foldl(fn ({channel, {ref, pid}}, acc) ->
      send(pid, message(:subscribed, %{channel: channel}))
      acc = MapSet.put(acc, channel)
      acc
    end, MapSet.new(), subscriptions) |> MapSet.to_list
    
    case channels == [] do
      true -> nil
      false -> :gen_tcp.send(state.socket, Enum.map([["SUBSCRIBE" | channels]], &Protocol.pack/1))
    end
  end

  defp flush_monitor_messages(ref) do
    receive do
      {:DOWN, ^ref, _, _, _} -> flush_monitor_messages(ref)
    after
      0 -> :ok
    end
  end

  defp log(state, action, message) do
    level =
      state.opts
      |> Keyword.fetch!(:log)
      |> Keyword.fetch!(action)

    Logger.log(level, message)
  end

  # TODO: remove once we depend on Elixir 1.4 and on.
  Code.ensure_loaded(Enum)

  split_with = if function_exported?(Enum, :split_with, 2), do: :split_with, else: :partition
  defp enum_split_with(enum, fun), do: apply(Enum, unquote(split_with), [enum, fun])
end
