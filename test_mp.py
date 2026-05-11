import asyncio, json, websockets

async def test():
    async with websockets.connect('ws://localhost:3000') as ws1:
        await ws1.send(json.dumps({'type': 'join', 'username': 'Alice'}))
        resp1 = json.loads(await asyncio.wait_for(ws1.recv(), 5))
        assert resp1['type'] == 'joined'
        alice_id = resp1['id']
        print(f'P1 Joined: {alice_id}, existing players: {len(resp1["players"])}')

        async with websockets.connect('ws://localhost:3000') as ws2:
            await ws2.send(json.dumps({'type': 'join', 'username': 'Bob'}))
            resp2 = json.loads(await asyncio.wait_for(ws2.recv(), 5))
            assert resp2['type'] == 'joined'
            bob_id = resp2['id']
            print(f'P2 Joined: {bob_id}, existing players: {len(resp2["players"])}')

            p1_notif = json.loads(await asyncio.wait_for(ws1.recv(), 5))
            assert p1_notif['type'] == 'player_join'
            assert p1_notif['username'] == 'Bob'
            print('P1 received player_join for Bob: OK')

            await ws1.send(json.dumps({'type': 'position', 'px': 10.5, 'py': 20.3, 'pz': 30.7, 'ry': 1.57, 'world': 'main'}))
            p2_pos = json.loads(await asyncio.wait_for(ws2.recv(), 5))
            assert p2_pos['type'] == 'position'
            assert abs(p2_pos['px'] - 10.5) < 0.01
            print(f'P2 received position: OK ({p2_pos["px"]},{p2_pos["py"]},{p2_pos["pz"]})')

            await ws1.send(json.dumps({'type': 'chat', 'message': 'Hello everyone!'}))
            p2_chat = json.loads(await asyncio.wait_for(ws2.recv(), 5))
            assert p2_chat['type'] == 'chat'
            assert p2_chat['username'] == 'Alice'
            assert p2_chat['message'] == 'Hello everyone!'
            p1_chat_back = json.loads(await asyncio.wait_for(ws1.recv(), 5))
            assert p1_chat_back['type'] == 'chat'
            print(f'Chat relay: OK ("{p2_chat["username"]}: {p2_chat["message"]}")')

            async with websockets.connect('ws://localhost:3000') as ws3:
                await ws3.send(json.dumps({'type': 'join', 'username': 'Alice'}))
                err = json.loads(await asyncio.wait_for(ws3.recv(), 5))
                assert err['type'] == 'error'
                print('Username taken detection: OK')

    print('\n=== ALL TESTS PASSED ===')

asyncio.run(test())
