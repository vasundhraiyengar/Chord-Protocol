defmodule Project3 do
  use GenServer
  def main(args) do
      numNodes=Enum.at(args, 0)|>String.to_integer()
      numRequests=Enum.at(args, 1)|>String.to_integer()
      
      m = :math.log(numNodes)/:math.log(2) |> :math.ceil |> round
      #IO.puts("m= #{m}")
      tabler= :ets.new(:tabler, [:named_table,:public])
      :ets.insert(tabler, {"m",m})

      tablee= :ets.new(:tablee, [:named_table,:public])
      :ets.insert(tablee, {"hop_count",0})

      x=trunc(:math.pow(2,m))-1
      node_collection = Enum.map((0..x), fn(i) ->
          i
      end)
      

      pid_collection=loop_start(0, numNodes, node_collection, [])
      #IO.inspect(pid_collection)
      hash_collection_uo = Enum.map((0..numNodes-1), fn(i) ->
          get_hash(Enum.fetch!(pid_collection, i))
        end)
      hash_collection=Enum.sort(hash_collection_uo)
      # IO.inspect(hash_collection)
      
      map=Enum.reduce pid_collection, %{}, fn x, acc ->
          Map.put(acc, get_hash(x), x)
      end
      # map=
      # Enum.each(pid_collection, fn(i) ->
      #     Map.put(map, get_hash(i), i)
      #     map
      # end)
      # IO.puts("map")
       #IO.inspect(hash_collection)
      len_hc=length(hash_collection)-1
      #IO.inspect(map)
      Enum.each(0..len_hc, fn(i) ->
          cond do
          i==0 -> 
              predecessor_hash=Enum.fetch!(hash_collection, length(hash_collection)-1)
              predecessor=Map.get(map,predecessor_hash)
              successor_hash=Enum.fetch!(hash_collection, i+1)
              successor=Map.get(map,successor_hash)
              current_pid=Map.get(map,Enum.fetch!(hash_collection, i))
              set_succ_pre(current_pid, successor, successor_hash, predecessor, predecessor_hash, hash_collection)

          i==len_hc ->
              predecessor_hash=Enum.fetch!(hash_collection, i-1)
              predecessor=Map.get(map,predecessor_hash)
              successor_hash=Enum.fetch!(hash_collection, 0)
              successor=Map.get(map,successor_hash)
              current_pid=Map.get(map,Enum.fetch!(hash_collection, i))
              set_succ_pre(current_pid, successor, successor_hash, predecessor, predecessor_hash, hash_collection)
              
           true ->
              predecessor_hash=Enum.fetch!(hash_collection, i-1)
              predecessor=Map.get(map,predecessor_hash)
              successor_hash=Enum.fetch!(hash_collection, i+1)
              successor=Map.get(map,successor_hash)
              current_pid=Map.get(map,Enum.fetch!(hash_collection, i))
              set_succ_pre(current_pid, successor, successor_hash, predecessor, predecessor_hash, hash_collection)
          end
      end)

      #Process.send(Map.get(map, Enum.fetch!(hash_collection,0)), :Fix_Fingers, [])
      
      fix_fingers(Map.get(map, Enum.fetch!(hash_collection,0)))
      cond do (numNodes<=100) ->
        :timer.sleep(1000)
        (100<numNodes && numNodes<=400) ->
        :timer.sleep(10000)
      true->
      :timer.sleep(60000)
     end

      
      find_keys(pid_collection, hash_collection, numRequests, numNodes)


  end  

  def find_keys(pid_collection, hash_collection, numRequests, numNodes) do
      # IO.puts("inside find keys")
      Enum.each(pid_collection, fn(i) ->
          Enum.each(1..numRequests, fn(j) ->
              key=Enum.random(hash_collection)
              #:timer.sleep(1000)
              find_successor_count(i, key, self())
              
              successor= receive do        
              {:successor_count, successor} -> #IO.puts("inside receive do successor")
              after
                      5000 -> #IO.puts("Timeout : search key: #{key}, searched by:")
                      #IO.inspect(i)
             end
           end)
      end)
      
      total_hop_count = elem(List.last(:ets.lookup(:tablee, "hop_count")),1)
      #-(numRequests*numNodes)
      #IO.inspect(total_hop_count)
      avg_hop_count=total_hop_count/(numNodes*numRequests)
      IO.puts("Average hops: #{avg_hop_count}")
      System.halt(0)
  end



  def start_link() do
      {:ok,pid}=GenServer.start_link(__MODULE__, [])
      pid
  end
  
  def init([]) do
      {:ok, {[],0,0,0,0,[]}}
  end

  def get_hash(pid) do
      GenServer.call(pid,:Get_Hash)
  end

  def handle_call(:Get_Hash, _from, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      hash=my_hash
      {:reply, hash, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}
  end

  def set_hash(pid,loc) do
      GenServer.cast(pid, {:Set_Hash, loc})
  end 

  def handle_cast({:Set_Hash,loc}, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      my_hash=loc
      {:noreply, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}   
  end

  def set_succ_pre(pid,successor1, successor1_hash, predecessor1, predecessor1_hash, hash1_collection) do
      GenServer.cast(pid, {:Set_Succ_Pre, successor1, successor1_hash, predecessor1, predecessor1_hash, hash1_collection})
  end 

  def handle_cast({:Set_Succ_Pre, successor1, successor1_hash, predecessor1, predecessor1_hash, hash1_collection}, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      successor=[id: successor1_hash, pid: successor1]
      predecessor=[id: predecessor1_hash, pid: predecessor1]
      {:noreply, {finger_list, my_hash, predecessor, successor, hop_count, hash1_collection}}   
  end


  def find_successor(pid, id, from_pid) do
      GenServer.call(pid,{:Find_Successor,id, from_pid})
  end

  def find_successor(self_pid,my_hash, finger_list, successor, id, from_pid) do
      #IO.puts("Find successor node #{my_hash} and finger list")
      #IO.inspect(finger_list)
      m = elem(List.last(:ets.lookup(:tabler, "m")),1)
      if (id==my_hash) do
          node=[id: my_hash, pid: self_pid]
          send(from_pid, {:successor, node})
      else
          if ((id > my_hash && id <= successor[:id]) || id==successor[:id] || (id < my_hash && id <= successor[:id])) do
              send(from_pid, {:successor, successor})
          else   
              cpn = closest_preceeding_node_try(finger_list, my_hash, id, successor)
              finger_list_cpn=get_finger_list(cpn[:pid])
              my_hash_cpn=cpn[:id]
              successor_cpn=get_successor(cpn[:pid])
              find_successor(cpn[:pid], my_hash_cpn, finger_list_cpn, successor_cpn, id, from_pid) 
          end
      end
  end

  def find_successor_count(pid, id, from_pid) do
      GenServer.call(pid,{:Find_Successor_Count,id, from_pid})
  end

  def handle_call({:Find_Successor_Count, id, from_pid}, _from, {finger_list, my_hash, predecessor, successor, hop_count, finger_count} ) do
      spawn(Project3, :find_successor_count, [self(), my_hash, finger_list, successor, id, from_pid])
      {:reply, :ok, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}
  end

  def find_successor_count(self_pid, my_hash, finger_list, successor, id, from_pid) do
      #IO.puts("inside find successor count finger list")
      #IO.inspect(finger_list)
      m = elem(List.last(:ets.lookup(:tabler, "m")),1)
      if (id==my_hash) do
          #hop_count = :ets.update_counter(:tablee, "hop_count", {2,1})
          send(from_pid, {:successor_count, self_pid})
      else
          if ((id > my_hash && id <= successor[:id]) || id==successor[:id]) do
              hop_count = :ets.update_counter(:tablee, "hop_count", {2,1})
              send(from_pid, {:successor_count, successor[:pid]})
          else   
              hop_count = :ets.update_counter(:tablee, "hop_count", {2,1})
              cpn = closest_preceeding_node_try(finger_list, my_hash, id, successor)
              finger_list_cpn=get_finger_list(cpn[:pid])
              #IO.inspect(finger_list_cpn)
              my_hash_cpn=cpn[:id]
              successor_cpn=get_successor(cpn[:pid])
              find_successor_count(self_pid, my_hash_cpn, finger_list_cpn, successor_cpn, id, from_pid)
          end
      end
  end

  def get_finger_list(pid) do
      GenServer.call(pid,:Get_Finger_List)
  end

  def handle_call(:Get_Finger_List, _from, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      {:reply, finger_list, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}
  end

  def get_successor(pid) do
      GenServer.call(pid,:Get_Successor)
  end

  def handle_call(:Get_Successor, _from, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      {:reply, successor, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}
  end


  def handle_call({:Find_Successor, id, from_pid}, _from, {finger_list, my_hash, predecessor, successor, hop_count, finger_count} ) do
      spawn(Project3, :find_successor, [self(), my_hash, finger_list, successor, id, from_pid])
      {:reply, :ok, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}}
  end


  def closest_preceeding_node_try(finger_list, my_hash, id, successor) do
      m = elem(List.last(:ets.lookup(:tabler, "m")),1)
      cpn=
      if (finger_list != []) do
          
          finger_hash_list=
          for i <- finger_list do
              i[:id]
          end
          
          list=
          Enum.flat_map finger_hash_list, fn j ->
              case j <= id do
              true -> [j]
              false -> []
              end
          end

          cpn_hash=
          if (list != []) do
              list |> Enum.max()
          else
              finger_hash_list |> Enum.max()
          end

          cpn1=
          for i <- finger_list do
              if (i[:id]==cpn_hash) do
                  i[:pid]
              end
          end
          # cpn1=
          # Enum.each(finger_list, fn( i) ->
          #     if (i[:id]==cpn_hash) do
          #         i[:pid]
          #     end
          # end)

          cpn2=Enum.filter(cpn1, & !is_nil(&1))
          cpn_pid=Enum.fetch!(cpn2, 0)

          cpn=[id: cpn_hash, pid: cpn_pid]
          cpn
      else
          successor
      end

      cpn
  end
  
  def fix_fingers(pid) do
      GenServer.cast(pid, :Fix_Fingers)
      #Process.send_after(pid,:Fix_Fingers, 1)
  end

  def handle_cast(:Fix_Fingers, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}) do
      #IO.inspect(successor)
      #IO.puts("inside fix fingers")
      m = elem(List.last(:ets.lookup(:tabler, "m")),1)
      y_h=
       if (successor[:pid]==self()) do
         my_hash
       else
          successor[:id]
         #get_hash(successor)
       end

      finger_list_up=
      for i <- 0..m-1 do
        id = (my_hash + :math.pow(2, i)) |> round
         id=
         if (id>(:math.pow(2,m)-1)) do
          rem(id, trunc(:math.pow(2,m)))
         else
           id
         end

         id=
         if (id > List.last(finger_count) && id < trunc(:math.pow(2,m))) do
          hd(finger_count)
         else
          id
         end

        #IO.puts("loop #{i}")
        result = if ((id > my_hash && id <= y_h) || (id==y_h)) do
          successor
          else
          #   #{:reply, cpn, {finger_list, my_hash, predecessor, successor, hop_count, finger_count}} = handle_call({:Closest_Preceeding_Node, id}, {finger_list, my_hash, predecessor, successor, hop_count, finger_count})
              cpn = closest_preceeding_node_try(finger_list, my_hash, id, successor)
              succ=
              if (cpn[:pid] != self()) do
                  find_successor(cpn[:pid], id, self())
                   successor= receive do
                      
                       {:successor, successor} -> #IO.puts("inside receive do fix fingers")
                       #IO.inspect(successor)
                       successor
                   after
                      5000 -> 
                   end
                  successor
              else
                  self()
              end
              succ
          end
          result
      end 
      #finger_list=finger_list_up
      #IO.puts("Current PID")
      #IO.inspect(self())
      # IO.puts("Node #{my_hash}")
      # IO.puts("updated finger list")
      # IO.inspect(finger_list_up)
      # #{finger_list, my_hash, predecessor, successor, hop_count, finger_count}={finger_list_up, my_hash, predecessor, successor, hop_count, finger_count}
      if (hop_count<1) do
          fix_fingers(successor[:pid])
      end
      
      { :noreply, {finger_list_up, my_hash, predecessor, successor, hop_count+1, finger_count} }
  end

  def loop_start(i, n, list, pid_collection) when i==n do
    pid_collection
  end

  def loop_start(i,n,list, pid_collection) do
      pid=start_link()
      pid_collection=pid_collection ++ [pid]
      
      loc=Enum.random(list)
      set_hash(pid, loc)
     
      loop_start(i+1, n, List.delete(list,loc), pid_collection)
  end
      
end
